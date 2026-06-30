"""
Sync agent for Vektor-Guard event drops.

Watches the event drop directory for new event_*.json files written by
the FastAPI service. Each file is uploaded to the Databricks landing
volume, then either archived locally (compressed, date-bucketed) or
moved to a failure directory with a sidecar diagnostic file.

A background sweeper migrates archived files older than the local TTL
to S3, where lifecycle tiering takes over -> One Zone-IA -> Glacier.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import shutil
import signal
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from io import BytesIO

import boto3
import structlog
from databricks.sdk import WorkspaceClient
from pydantic import BaseModel
from watchdog.events import FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver


class SyncConfig(BaseModel):
    """Runtime configuration for the sync agent, loaded from environment."""

    log_level: str = "INFO"
    aws_region: str = "us-west-2"

    # Filesystem tiers
    event_drop_dir: Path
    shipped_dir: Path
    failed_dir: Path
    local_ttl_hours: int = 24

    # S3 archive
    s3_bucket: str
    s3_prefix: str = "events/"

    # Databricks landing
    databricks_volume_path: str = "/Volumes/vektor_guard_dp/bronze/landing"
    databricks_pat_secret_id: str = "vektor-guard-dp-dev/databricks-pat"
    databricks_workspace_url_param: str = "/vektor-guard-dp-dev/databricks/workspace-url"

    # Sweeper tuning
    sweeper_interval_seconds: int = 300
    max_upload_retries: int = 3

    # Observer mode - polling for Docker Desktop on macOS (inotify does not
    # propagate across the virtualized filesystem), native inotify on Linux.
    use_polling_observer: bool = False


def load_config() -> SyncConfig:
    """Build SyncConfig from environment variables."""
    return SyncConfig(
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        aws_region=os.environ.get("AWS_REGION", "us-west-2"),
        event_drop_dir=Path(os.environ["EVENT_DROP_DIR"]),
        shipped_dir=Path(os.environ["SHIPPED_DIR"]),
        failed_dir=Path(os.environ["FAILED_DIR"]),
        local_ttl_hours=int(os.environ.get("LOCAL_TTL_HOURS", "24")),
        s3_bucket=os.environ["S3_BUCKET"],
        s3_prefix=os.environ.get("S3_PREFIX", "events/"),
        databricks_volume_path=os.environ.get(
            "DATABRICKS_VOLUME_PATH", "/Volumes/vektor_guard_dp/bronze/landing"
        ),
        databricks_pat_secret_id=os.environ.get(
            "DATABRICKS_PAT_SECRET_ID", "vektor-guard-dp-dev/databricks-pat"
        ),
        databricks_workspace_url_param=os.environ.get(
            "DATABRICKS_WORKSPACE_URL_PARAM",
            "/vektor-guard-dp-dev/databricks/workspace-url",
        ),
        sweeper_interval_seconds=int(os.environ.get("SWEEPER_INTERVAL_SECONDS", "300")),
        max_upload_retries=int(os.environ.get("MAX_UPLOAD_RETRIES", "3")),
        use_polling_observer=os.environ.get("USE_POLLING_OBSERVER", "false").lower()
        == "true",
    )


def get_secret(secret_id: str, region: str) -> str:
    """Fetch a secret string from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_id)
    return response["SecretString"]


def get_ssm_parameter(name: str, region: str) -> str:
    """Fetch a parameter value from AWS SSM Parameter Store."""
    client = boto3.client("ssm", region_name=region)
    response = client.get_parameter(Name=name)
    return response["Parameter"]["Value"]


def build_databricks_client(config: SyncConfig) -> WorkspaceClient:
    """Construct a Databricks WorkspaceClient from PAT + workspace URL.

    PAT is read from Secrets Manager, workspace URL from SSM Parameter Store -
    same pattern proven in the databricks smoke test.
    """
    pat = get_secret(config.databricks_pat_secret_id, config.aws_region)
    workspace_url = get_ssm_parameter(
        config.databricks_workspace_url_param, config.aws_region
    )
    return WorkspaceClient(host=workspace_url, token=pat)


def archive_shipped(event_path: Path, shipped_dir: Path) -> Path:
    """Gzip a shipped event into the date-bucketed local hot tier.

    Compresses event_path into shipped_dir/YYYY/MM/DD/<name>.json.gz using a
    temp-plus-rename so a partially written archive is never observable, then
    removes the original. Returns the final archive path.
    """
    now = datetime.now(UTC)
    dest_dir = shipped_dir / f"{now:%Y}" / f"{now:%m}" / f"{now:%d}"
    dest_dir.mkdir(parents=True, exist_ok=True)

    final_path = dest_dir / f"{event_path.name}.gz"
    tmp_path = final_path.with_suffix(".gz.tmp")

    with event_path.open("rb") as src, gzip.open(tmp_path, "wb") as dst:
        shutil.copyfileobj(src, dst)

    os.replace(tmp_path, final_path)
    event_path.unlink()

    return final_path


def capture_failure(event_path: Path, failed_dir: Path, error: Exception) -> Path:
    """Quarantine a failed event into the local failure tier with a sidecar.

    Moves event_path (uncompressed) into failed_dir/YYYY/MM/DD/ and writes a
    <name>.failure JSON sidecar capturing the error, timestamp, and original
    path. These stay local forever for manual inspection - no S3, no TTL.
    Returns the final quarantined event path.
    """
    now = datetime.now(UTC)
    dest_dir = failed_dir / f"{now:%Y}" / f"{now:%m}" / f"{now:%d}"
    dest_dir.mkdir(parents=True, exist_ok=True)

    final_path = dest_dir / event_path.name
    sidecar_path = final_path.with_suffix(final_path.suffix + ".failure")

    metadata = {
        "original_path": str(event_path),
        "failed_at": now.isoformat(),
        "error_type": type(error).__name__,
        "error_message": str(error),
    }

    shutil.move(str(event_path), str(final_path))

    tmp_sidecar = sidecar_path.with_suffix(".failure.tmp")
    tmp_sidecar.write_text(json.dumps(metadata, indent=2))
    os.replace(tmp_sidecar, sidecar_path)

    return final_path


class EventHandler(FileSystemEventHandler):
    """Watchdog handler that ships new event files to Databricks.

    On each created event_*.json: read and validate, upload to the Databricks
    landing volume with bounded retries, then archive on success or quarantine
    on failure.
    """

    def __init__(
        self,
        config: SyncConfig,
        databricks: WorkspaceClient,
        log: structlog.BoundLogger,
    ) -> None:
        self.config = config
        self.databricks = databricks
        self.log = log

    def on_created(self, event: FileCreatedEvent) -> None:
        if event.is_directory:
            return

        path = Path(event.src_path)
        if not path.name.startswith("event_") or path.suffix != ".json":
            return

        self.process_event(path)

    def process_event(self, path: Path) -> None:
        log = self.log.bind(event_file=path.name)
        try:
            payload = path.read_bytes()
            json.loads(payload)  # validate it parses before shipping

            self.upload_with_retry(path, payload)

            archived = archive_shipped(path, self.config.shipped_dir)
            log.info("event_shipped", archive=str(archived))

        except FileNotFoundError:
            log.warning("event_vanished")
        except Exception as e:
            quarantined = capture_failure(path, self.config.failed_dir, e)
            log.error("event_failed", quarantine=str(quarantined), error=str(e))

    def upload_with_retry(self, path: Path, payload: bytes) -> None:
        target = f"{self.config.databricks_volume_path}/{path.name}"
        last_error: Exception | None = None

        for attempt in range(1, self.config.max_upload_retries + 1):
            try:
                self.databricks.files.upload(
                    file_path=target,
                    contents=BytesIO(payload),
                    overwrite=True,
                )
                return
            except Exception as e:
                last_error = e
                self.log.warning(
                    "upload_attempt_failed",
                    event_file=path.name,
                    attempt=attempt,
                    error=str(e),
                )
                time.sleep(2 ** (attempt - 1))

        raise last_error  # exhausted retries, let process_event quarantine it


class S3MigrationSweeper(threading.Thread):
    """Background thread migrating aged-out shipped files to S3.

    On a fixed interval, scans the local shipped tier for *.json.gz files older
    than LOCAL_TTL_HOURS, uploads each to s3://BUCKET/PREFIX/YYYY/MM/DD/, then
    deletes the local copy. S3 lifecycle policy handles tiering from there.
    """

    def __init__(
        self,
        config: SyncConfig,
        s3_client: boto3.client,
        log: structlog.BoundLogger,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name="s3-sweeper", daemon=True)
        self.config = config
        self.s3 = s3_client
        self.log = log
        self.stop_event = stop_event

    def run(self) -> None:
        self.log.info("sweeper_started", interval=self.config.sweeper_interval_seconds)
        while not self.stop_event.is_set():
            try:
                self.sweep_once()
            except Exception as e:
                self.log.error("sweep_cycle_failed", error=str(e))
            self.stop_event.wait(self.config.sweeper_interval_seconds)
        self.log.info("sweeper_stopped")

    def sweep_once(self) -> None:
        cutoff = datetime.now(UTC) - timedelta(hours=self.config.local_ttl_hours)
        for path in self.config.shipped_dir.rglob("*.json.gz"):
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
            if mtime > cutoff:
                continue
            self.migrate_file(path)

    def migrate_file(self, path: Path) -> None:
        rel = path.relative_to(self.config.shipped_dir)
        key = f"{self.config.s3_prefix}{rel.as_posix()}"
        log = self.log.bind(local=str(path), s3_key=key)
        try:
            self.s3.upload_file(str(path), self.config.s3_bucket, key)
            path.unlink()
            log.info("migrated_to_s3")
        except Exception as e:
            log.error("migration_failed", error=str(e))


def build_observer(config: SyncConfig) -> BaseObserver:
    """Select the watchdog observer backend.

    PollingObserver scans on a timer and works across virtualized filesystems
    (Docker Desktop on macOS), where inotify events do not propagate into the
    container. Native Observer (inotify) is correct on Linux hosts / EC2.
    """
    if config.use_polling_observer:
        from watchdog.observers.polling import PollingObserver

        return PollingObserver()
    return Observer()


def main() -> None:
    """Entry point: start the watchdog observer and S3 sweeper, run until signaled."""
    config = load_config()

    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[config.log_level]
        ),
    )
    log = structlog.get_logger("sync_agent")

    # Ensure local tiers exist before anything watches or writes them
    config.event_drop_dir.mkdir(parents=True, exist_ok=True)
    config.shipped_dir.mkdir(parents=True, exist_ok=True)
    config.failed_dir.mkdir(parents=True, exist_ok=True)

    databricks = build_databricks_client(config)
    s3_client = boto3.client("s3", region_name=config.aws_region)
    log.info(
        "clients_ready",
        volume=config.databricks_volume_path,
        bucket=config.s3_bucket,
    )

    stop_event = threading.Event()

    def handle_signal(signum: int, frame: object) -> None:
        log.info("shutdown_signal", signal=signal.Signals(signum).name)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    sweeper = S3MigrationSweeper(config, s3_client, log, stop_event)
    sweeper.start()

    handler = EventHandler(config, databricks, log)
    observer = build_observer(config)
    observer.schedule(handler, str(config.event_drop_dir), recursive=False)
    observer.start()
    log.info(
        "sync_agent_running",
        watching=str(config.event_drop_dir),
        polling=config.use_polling_observer,
    )

    try:
        while not stop_event.is_set():
            stop_event.wait(1.0)
    finally:
        log.info("sync_agent_stopping")
        observer.stop()
        observer.join()
        sweeper.join(timeout=10)
        log.info("sync_agent_stopped")


if __name__ == "__main__":
    main()