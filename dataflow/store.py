"""S3 interface, prefix-based releases, independent of machine filesystem."""

import json
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .settings import Settings


def client(settings: Settings | None = None) -> Any:
    """Create an S3 client using explicit settings or environment credentials."""
    settings = settings or Settings.from_env()
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint,
        aws_access_key_id=settings.access_key,
        aws_secret_access_key=settings.secret_key,
        region_name=settings.region,
        verify=settings.secure,
        config=Config(
            signature_version="s3v4", retries={"max_attempts": 8, "mode": "standard"},
        ),
    )


def json_write(s3: Any, bucket: str, key: str, data: Any) -> None:
    """Serialize and upload a JSON document without caching."""
    payload = json.dumps(data, indent=2, sort_keys=True, allow_nan=False).encode(
        "utf-8",
    )
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType="application/json",
        CacheControl="no-store",
    )


def json_read(s3: Any, bucket: str, key: str) -> Any:
    """Read and decode a JSON object from S3."""
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def exists(s3: Any, bucket: str, key: str) -> bool:
    """Check object existence, propagating errors other than missing keys."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    else:
        return True
