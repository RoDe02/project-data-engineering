"""Strictly checked settings, shared by all data-plane processes."""

import os
import re
from dataclasses import dataclass

UCI_URL = (
    "https://archive.ics.uci.edu/static/public/1050/twitter%2Bgeospatial%2Bdata.zip"
)
DATASET_ROWS = 14_262_517
MAX_BAD_FRACTION = 0.01


def valid_run_id(run_id: str) -> str:
    """Validate and return a run identifier safe for object keys."""
    if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,99}", run_id):
        msg = "run_id must match [a-z0-9][a-z0-9_.-]{0,99}"
        raise ValueError(msg)
    return run_id


@dataclass(frozen=True)
class Settings:
    """Immutable connection settings for the object store."""

    endpoint: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    secure: bool = False

    @classmethod
    def from_env(cls) -> "Settings":
        """Load the S3 endpoint, credentials and TLS settings from the environment."""
        return cls(
            endpoint=os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
            access_key=os.environ["AWS_ACCESS_KEY_ID"],
            secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            secure=os.environ.get("S3_VERIFY_TLS", "false").lower() == "true",
        )
