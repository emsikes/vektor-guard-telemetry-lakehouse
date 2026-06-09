"""
FastAPI inference endpoint for Vektor-Guard.

Serves predictions from the Vektor-Guard prompt injection / jailbreak
classifier.  Each inference request produces a JSON event written to
EVENT_DROP_DIR for downstream sync to the Databricks lakehouse.

Supports two modes via INFERENCE_MODE env var:
- 'real': loads theinferenceloop/vektor-guard-v2 from HuggingFace
- 'stub': returns hardocoded predictions for dev/integration testing
"""

from __future__ import annotations

import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

import structlog
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

InferenceMode = Literal["real", "stub"]


class AppConfig(BaseModel):
    """Configuration loaded from environment variables."""

    log_level: str = Field(default="INFO")
    inference_mode: InferenceMode = Field(default="stub")
    model_name: str = Field(default="theinferenceloop/vektor-guard-v2")
    event_drop_dir: Path = Field(default=Path("/var/run/vektor-guard/events"))
    max_text_length: int = Field(default=2048, ge=1, le=10_000)


def load_config() -> AppConfig:
    """Read configuration from environment with safe defaults."""
    raw_mode = os.environ.get("INFERENCE_MODE", "stub").lower()
    if raw_mode not in ("real", "stub"):
        raise ValueError(
            f"INFERENCE_MODE must be 'real' or 'stub', got {raw_mode!r}"
        )
    
    return AppConfig(
        log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        inference_mode=raw_mode,
        model_name=os.environ.get("MODEL_NAME", "theinferenceloop/vektor-guard-v2"),
        event_drop_dir=Path(
            os.environ.get("EVENT_DROP_DIR", "/var/run/vektor-guard/events")
        ),
        max_text_length=int(os.environ.get("MAX_TEXT_LENGTH", "2048"))
    )

# -----------------------------------------------------------------------------
# API contract - request and response shapes
# -----------------------------------------------------------------------------

class InferenceRequest(BaseModel):
    """Payload sent to /infer."""

    text: str = Field(
        ...,
        min_length=1,
        max_length=10_000,
        description="The prompt text classify",
    )
    session_id: str = Field(
        default=None,
        description="Optional session identifier for grouping related calls.",
    )
    metadata: dict[str, str] | None = Field(
        default=None,
        description="Optional opaque metadata attached to the event record",
    )


class InferenceResponse(BaseModel):
    """Response returned from /infer."""

    event_id: str
    event_ts: str
    predicted_class: Literal[
        "clean",
        "instruction_override",
        "indirect_injection",
        "jailbreak",
        "tool_call_hijacking",
    ]
    predicted_confidence: float = Field(ge=0.0, le=1.0)
    model_name: str
    inference_mode: InferenceMode


# -----------------------------------------------------------------------------
# Inference engine - real model and stub implementations
# -----------------------------------------------------------------------------

class InferenceEngine:
    """Abstract interface for prediction backends."""

    def predict(self, text: str) -> tuple[str, float]:
        """
        Classify a text input.

        Returns:
            (predicted_class, confidence) where class is one of the
            five Vektor-Guard taxonomy classes and confidence is in [0, 1].
        """
        raise NotImplementedError("Subclasses must implement predict()")
    

class StubEngine(InferenceEngine):
    """
    Stub inference engine for dev and integration testing.

    Returns hardcoded predictions based on simple keyword matching.
    No model download, no inference latency, deterministic output.
    """

    _KEYWORD_MAP: dict[str, str] = {
        "ignore previous": "instruction_override",
        "ignore your": "instruction_override",
        "system prompt": "instruction_override",
        "{{": "indirect_injection",
        "</user": "indirect_injection",
        "do anything now": "jailbreak",
        "dan mode": "jailbreak",
        "developer mode": "jailbreak",
        "call this tool": "tool_call_hijacking",
        "execute the following": "tool_call_hijacking",
    }

    def predict(self, text: str) -> tuple[str, float]:
        lowered = text.lower()
        for keyword, label in self._KEYWORD_MAP.items():
            if keyword in lowered:
                return label, 0.95
            
        # Default - looks clean
        return "clean", 0.99
    

class RealEngine(InferenceEngine):
    """
    Production inference engine using the real Vektor-Guard model.

    Loads ModernBERT-large fine-tuned on the Vektor-Guard taxonomy from
    HuggingFace Hub.  Lazy initialization - model loads on first predict()
    call to keep container startup fast.
    """

    _CLASS_LABELS = [
        "clean",
        "instruction_override",
        "indirect_injection",
        "jailbreak",
        "tool_call_hijacking",
    ]

    def __init__(self, model_name: str, max_text_length: int) -> None:
        self.model_name = model_name
        self._max_text_length = max_text_length
        self._tokenizer = None
        self._model = None

    def _ensure_loaded(self) -> None:
        """Lazy-load the model on first use, imports here so we only load when needed."""
        if self._model is not None:
            return
        
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
        
        self._tokenizer = AutoTokenizer.from_pretrained(self._model_name)
        self._model = AutoModelForSequenceClassification.from_pretrained(
            self._model_name
        )
        self._model.eval()

    def predict(self, text: str) -> tuple[str, float]:
        import torch

        self._ensure_loaded()

        inputs = self._tokenizer(  # type: ignore[misc]
            text,
            return_tensors="pt",
            truncation=True,
            max_length=self._max_text_length,
        )

        with torch.no_grad():
            outputs = self._model(**inputs) # type: ignore[misc]

        logits = outputs.logits
        probs = torch.softmax(logits, dim=-1)[0]
        predicted_idx = int(probs.argmax().item())
        confidence = float(probs[predicted_idx].item())
        predicted_class = self._CLASS_LABELS[predicted_idx]

        return predicted_class, confidence
    
def build_engine(config: AppConfig) -> InferenceEngine:
    """Construct the appropriate engine based on configured mode."""
    if config.inference_mode == "stub":
        return StubEngine()
    return RealEngine(
        model_name=config.model_name,
        max_text_length=config.max_text_length,
    )

# -----------------------------------------------------------------------------
# FastAPI application
# -----------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup and shutdown handler.

    On startup: load config, build the inference engine, ensure the event drop
    directory exists, attach everything to app.state.

    On shutdown: nothing to do (engines hold no resources we need to free back up)
    """
    config = load_config()
    engine = build_engine(config)

    config.event_drop_dir.mkdir(parents=True, exist_ok=True)

    logger = structlog.get_logger()
    logger.info(
        "fastapi.startup",
        inference_mode=config.inference_mode,
        model_name=config.model_name,
        event_drop_dir=config.event_drop_dir,
        log_level=config.log_level,
    )

    app.state.config = config
    app.state.engine = engine
    app.state.logger = logger
    
    yield

    logger.info("fastapi.shutdown")

app = FastAPI(
    title="Vektor-Guard Inference",
    description="Prompt injection / jailbreak classifier with event capture",
    version="0.1.0",
    lifespan=lifespan,
)

@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe - always returns ok if the process is responsive."""
    return {"status": "ok"}

@app.get("/ready")
async def ready(request: Request) -> dict[str, str]:
    """Readiness probe - confirms the engine is constructed and ready."""
    if not hasattr(request.app.state, "engine"):
        raise HTTPException(status_code=503, detail="engine not initialized")
    return {"status": "ready"}

# -----------------------------------------------------------------------------
# Event sink - writes inference events to the drop directory
# -----------------------------------------------------------------------------

def write_event_to_sink(
    event_drop_dir: Path,
    event_id: str,
    event_ts: str,
    request_payload: InferenceRequest,
    predicted_class: str,
    predicted_confidence: float,
    model_name: str,
    inference_mode: InferenceMode,
) -> Path:
    """
    Write a single inference event to the drop directory as JSON.

    Returns the path the event was written to.  Performs a write-then-rename
    atomic operation to prevent the sync agent from reading partial files.
    """
    record = {
        "event_id": event_id,
        "event_ts": event_ts,
        "event_source": "live",
        "model_name": model_name,
        "inference_mode": inference_mode,
        "text": request_payload.text,
        "session_id": request_payload.session_id,
        "metadata": request_payload.metadata or {},
        "predicted_class": predicted_class,
        "predicted_confidence": predicted_confidence,
        "provenance": {
            "service": "fastapi",
            "service_version": "0.1.0",
        },
    }

    final_path = event_drop_dir / f"event_{event_id}.json"
    temp_path = final_path.with_suffix(".json.tmp")

    temp_path.write_text(json.dumps(record, separators=(",",":")) + "\n")
    temp_path.rename(final_path)

    return final_path

@app.post("/infer", response_model=InferenceResponse)
async def infer(
    payload: InferenceRequest,
    request: Request,
) -> InferenceResponse:
    """
    Classify a text input and emit a telemetry event.

    Predicts the class via the configured inference engine, writes the 
    full event record to the drop directory for downstream sync, and
    returns the prediction to the caller.
    """
    config: AppConfig = request.app.state.config
    engine: InferenceEngine = request.app.state.engine
    logger = request.app.state.logger

    if len(payload.text) > config.max_text_length:
        raise HTTPException(
            status_code=413,
            detail=f"text exceeds max_text_length of {config.max_text_length}",
        )
    
    event_id = str(uuid.uuid4())
    event_ts = datetime.now(UTC).isoformat()

    start = time.perf_counter()
    predicted_class, predicted_confidence = engine.predict(payload.text)
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    sink_path = write_event_to_sink(
        event_drop_dir=config.event_drop_dir,
        event_id=event_id,
        event_ts=event_ts,
        request_payload=payload,
        predicted_class=predicted_class,
        predicted_confidence=predicted_confidence,
        model_name=config.model_name,
        inference_mode=config.inference_mode,
    )

    logger.info(
        "infer.complete",
        event_id=event_id,
        predicted_class=predicted_class,
        predicted_confidence=round(predicted_confidence, 4),
        inference_ms=round(elapsed_ms, 2),
        sink_path=str(sink_path),
    )

    return InferenceResponse(
        event_id=event_id,
        event_ts=event_ts,
        predicted_class=predicted_class,  # type: ignore[arg-type]
        predicted_confidence=predicted_confidence,
        model_name=config.model_name,
        inference_mode=config.inference_mode,
    )

# -----------------------------------------------------------------------------
# Entry point - invoked by the 'vektor-guard-fastapi' console script
# -----------------------------------------------------------------------------

def main() -> None:
    """Run the FastAPI service via uvicorn."""
    import uvicorn

    config = load_config()

    structlog.configure(
        processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(__import__("logging"), config.log_level)
        ),
    )

    uvicorn.run(
        "vektor_guard_runtime.fastapi_app:app",
        host="0.0.0.0",  # noqa: S104 - intentional bind for container deployment
        port=8000,
        log_config=None
    )