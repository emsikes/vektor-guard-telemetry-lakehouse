# S3 events archive bucket
#
# Durability backstop for runtime inference events.  Primary ingestion path is
# Databricks Auto Loader from a Unity Catalog Volume, but every event also lands
# here for replay capability and independent audit trail.
#
# Security posture: No public access, SSE-S3 encryption, TLS only, versioning enabled,
# lifecycle to Glacier Instant Retrieval after 90 days.

resource "aws_s3_bucket" "events" {
  bucket = "${local.name_prefix}-events"
}

# Block all public access.  Defense in depth, we never set an ACL or public
# policies but this is a catch all in case of a missed config setting or breech attempt
resource "aws_s3_bucket_public_access_block" "events" {
  bucket = aws_s3_bucket.events.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Server side encryption with S3 managed keys (AES-256)
resource "aws_s3_bucket_server_side_encryption_configuration" "events" {
  bucket = aws_s3_bucket.events.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Enable versioning to protect against accidental deletes or overwrites
resource "aws_s3_bucket_versioning" "events" {
  bucket = aws_s3_bucket.events.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Lifecycle: transition to Glacier Instant Retrieval after 90 days
# Short active replay window, long-term retention for auditing
resource "aws_s3_bucket_lifecycle_configuration" "events" {
  bucket = aws_s3_bucket.events.id

  rule {
    id     = "events-archive-tiering"
    status = "Enabled"

    filter {
      prefix = "events/"
    }

    # Day 30: move out of Standard into One Zone-IA
    # (events are reproducible from the Databricks bronze table, so
    # we don't pay for cross-AZ redundancy we don't need)
    transition {
      days          = 30
      storage_class = "ONEZONE_IA"
    }

    # Day 90: move to Glacier Instant Retrieval for the deep archive
    # (cheaper than One Zone-IA at the long horizon, ms retrieval latency)
    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    # Day 365: expire entirely
    expiration {
      days = 365
    }
  }
}


# Bucket policy require TLS for all requests.  Even though bucket is private,
# this prevents accidental plain text access if a future IAM grant or
# misconfiguration exposes a path.
data "aws_iam_policy_document" "events_bucket_policy" {
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.events.arn,
      "${aws_s3_bucket.events.arn}/*",
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "events" {
  bucket = aws_s3_bucket.events.id
  policy = data.aws_iam_policy_document.events_bucket_policy.json
}