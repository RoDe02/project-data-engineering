"""Consumer-side release contract: no access to raw GPS is needed."""

import argparse
import json
import sys
from collections.abc import Iterator
from contextlib import closing
from typing import Any
from uuid import uuid4

from botocore.exceptions import ClientError

from .store import client, json_read


def require_identity(
    manifest: dict[str, Any],
    run_id: str,
    source_date: str,
    source_version: str,
) -> None:
    """Require a manifest to identify the requested batch exactly."""
    expected = {
        "run_id": run_id,
        "source_date_cst": source_date,
        "source_version": source_version,
    }
    for field, value in expected.items():
        if manifest.get(field) != value:
            msg = f"Manifest {field} does not match the requested batch"
            raise ValueError(msg)


def verify(
    s3: Any = None,
    *,
    run_id: str | None = None,
    source_date: str | None = None,
    source_version: str | None = None,
) -> dict[str, Any]:
    """Validate the latest Gold release and optionally enforce its batch identity."""
    s3 = s3 or client()
    manifest = json_read(s3, "gold", "releases/latest.json")
    if any(value is not None for value in (run_id, source_date, source_version)):
        if not all((run_id, source_date, source_version)):
            msg = (
                "Verification requires run ID, source date and source version together"
            )
            raise ValueError(msg)
        require_identity(manifest, run_id, source_date, source_version)
    release_key = (
        f"daily/date={manifest['source_date_cst']}/runs/{manifest['run_id']}/release.json"
        if "source_date_cst" in manifest
        else f"releases/{manifest['run_id']}.json"
    )
    release = json_read(s3, "gold", release_key)
    if release != manifest:
        msg_0 = "Latest release differs from immutable release"
        raise ValueError(msg_0)
    verify_features(s3, manifest)
    return manifest


def verify_features(s3: Any, manifest: dict[str, Any]) -> None:
    """Check the feature URI a consumer will use before promoting the release."""
    if manifest["tweet_count_total"] <= 0 or manifest["feature_rows"] <= 0:
        msg = "Empty release"
        raise ValueError(msg)
    prefix = (
        f"daily/date={manifest['source_date_cst']}/runs/{manifest['run_id']}/features/"
        if "source_date_cst" in manifest
        else f"runs/{manifest['run_id']}/features/"
    )
    # Pre-daily releases could omit feature_path and used the conventional runs/ prefix.
    path = manifest.get("feature_path")
    if ("source_date_cst" in manifest or path is not None) and (
        not isinstance(path, str) or path.rstrip("/") != f"s3a://gold/{prefix[:-1]}"
    ):
        msg_0 = "Feature path does not match the released prefix"
        raise ValueError(msg_0)
    if not has_parquet(s3, "gold", prefix):
        msg_1 = "No Parquet files found in released prefix"
        raise ValueError(msg_1)


def has_parquet(s3: Any, bucket: str, prefix: str) -> bool:
    """Return whether the prefix contains at least one Parquet object."""
    return next(parquet_objects(s3, bucket, prefix), None) is not None


def parquet_objects(s3: Any, bucket: str, prefix: str) -> Iterator[dict[str, Any]]:
    """Yield Parquet object metadata across all listing pages."""
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                yield obj


def check_reader_access(s3: Any, release: dict[str, Any]) -> None:
    """Exercise the actual consumer identity after a dated release has been verified."""
    root = f"daily/date={release['source_date_cst']}/runs/{release['run_id']}"
    part = next(parquet_objects(s3, "gold", f"{root}/features/"), None)
    if part is None:
        msg = "Released Gold Parquet file is missing"
        raise ValueError(msg)
    response = s3.get_object(Bucket="gold", Key=part["Key"], Range="bytes=0-3")
    with closing(response["Body"]) as body:
        if body.read() != b"PAR1":
            msg_0 = "Gold reader could not read a Parquet file header"
            raise ValueError(msg_0)
    print("PASS: Gold Parquet header readable (PAR1)", file=sys.stderr, flush=True)

    # Use GET so MinIO returns the error code. HEAD can reduce it to a generic 403.
    try:
        response = s3.get_object(Bucket="silver", Key=f"{root}/manifest.json")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "AccessDenied":
            raise
        print(f"PASS: Silver read denied: {exc}", file=sys.stderr, flush=True)
    else:
        response["Body"].close()
        msg_1 = "Gold reader unexpectedly read the Silver manifest"
        raise PermissionError(msg_1)

    probe = f"access-checks/{uuid4().hex}"
    try:
        s3.put_object(Bucket="gold", Key=probe, Body=b"")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "AccessDenied":
            raise
        print(f"PASS: Gold write denied: {exc}", file=sys.stderr, flush=True)
    else:
        msg_2 = (
            f"Gold reader unexpectedly wrote gold/{probe}. Remove the probe as operator"
        )
        raise PermissionError(msg_2)


def main() -> None:
    """Verify a Gold release and optionally check consumer access permissions."""
    parser = argparse.ArgumentParser(description="Verify the latest Gold release")
    parser.add_argument("--run-id")
    parser.add_argument("--source-date")
    parser.add_argument("--source-version")
    parser.add_argument(
        "--check-reader-access",
        action="store_true",
        help="Read a Gold Parquet header and confirm Silver read and Gold write are denied",
    )
    args = parser.parse_args()
    s3 = client()
    release = verify(
        s3,
        run_id=args.run_id,
        source_date=args.source_date,
        source_version=args.source_version,
    )
    if args.check_reader_access:
        check_reader_access(s3, release)
    print(json.dumps(release, indent=2))


if __name__ == "__main__":
    main()
