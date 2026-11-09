"""Download and checksum the original UCI Twitter geospatial archive."""

import hashlib
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .settings import UCI_URL


def download(dst: Path, limit: int = 1_000_000_000) -> str:
    """Download the original UCI archive and return its SHA-256 digest."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    sha = hashlib.sha256()
    count = 0
    with session.get(
        UCI_URL,
        stream=True,
        timeout=(20, 180),
        allow_redirects=True,
    ) as response:
        response.raise_for_status()
        with dst.open("wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    count += len(chunk)
                    if count > limit:
                        msg = "Source exceeds download size limit"
                        raise ValueError(msg)
                    sha.update(chunk)
                    output.write(chunk)
    if count < 100:
        msg_0 = "Source download unexpectedly short"
        raise ValueError(msg_0)
    return sha.hexdigest()
