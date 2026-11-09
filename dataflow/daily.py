"""One-day-at-a-time simulation from the historical UCI archive.

Only the source archive is cached. A day is materialized when manually triggered.
Object layout: bronze/source/archive.zip, bronze/daily/date=YYYY-MM-DD/...
"""

import argparse
import csv
import hashlib
import json
import os
import tempfile
from datetime import date, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any, TextIO

from dataflow.ingest import download
from dataflow.settings import DATASET_ROWS, MAX_BAD_FRACTION, UCI_URL, valid_run_id
from dataflow.source import iter_normalized_rows
from dataflow.store import client, exists, json_read, json_write

DAYS = tuple((date(2013, 1, 12) + timedelta(days=n)).isoformat() for n in range(7))
ARCHIVE_KEY = "source/archive.zip"
SOURCE_META = "source/archive.json"


def source_day(timestamp: str) -> str | None:
    """Extract an ISO source date from a supported timestamp."""
    value = timestamp.strip()
    for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def prepare_source(s3: Any) -> dict[str, Any]:
    """Reuse or download the source archive and return its metadata."""
    if exists(s3, "bronze", SOURCE_META):
        meta = json_read(s3, "bronze", SOURCE_META)
        if not exists(s3, "bronze", ARCHIVE_KEY):
            msg = "Committed source metadata references a missing archive"
            raise ValueError(msg)
        if meta.get("expected_rows") != DATASET_ROWS:
            msg_0 = "Source archive metadata expected-row mismatch"
            raise ValueError(msg_0)
        if meta.get("source_url") != UCI_URL or meta.get("raw_key") != ARCHIVE_KEY:
            msg_1 = "Cached source metadata does not identify the original UCI archive"
            raise ValueError(msg_1)
        return meta
    with tempfile.TemporaryDirectory(prefix="twitter-source-") as td:
        archive = Path(td) / "archive.zip"
        digest = download(archive)
        s3.upload_file(str(archive), "bronze", ARCHIVE_KEY)
        meta = {
            "schema_version": 2,
            "sha256": digest,
            "source_url": UCI_URL,
            "expected_rows": DATASET_ROWS,
            "raw_key": ARCHIVE_KEY,
        }
        json_write(s3, "bronze", SOURCE_META, meta)
        return meta


class _CsvParts:
    """Upload bounded CSV parts and reuse the same local spool file."""

    def __init__(self, s3: Any, prefix: str, spool: TextIO, part_rows: int) -> None:
        """Initialize a reusable CSV spool and its upload destination."""
        self.s3 = s3
        self.prefix = prefix
        self.spool = spool
        self.writer = csv.writer(spool)
        self.part_rows = part_rows
        self.count = 0
        self.keys: list[str] = []

    def append(self, row: list[str]) -> None:
        """Append a row and upload the part when its row limit is reached."""
        self.writer.writerow(row)
        self.count += 1
        if self.count % self.part_rows == 0:
            self.flush()

    def flush(self) -> None:
        """Upload a nonempty CSV part and reset the spool."""
        if not self.spool.tell():
            return
        self.spool.flush()
        key = f"{self.prefix}/part-{len(self.keys):05d}.csv"
        self.s3.upload_file(self.spool.name, "bronze", key)
        self.keys.append(key)
        self.spool.seek(0)
        self.spool.truncate()


def _scan_archive(
    s3: Any,
    archive: Path,
    day: str,
    quarantine_prefix: str,
    expected_rows: int,
    *,
    publish_day: bool,
    publish_quarantine: bool,
    part_rows: int = 250_000,
) -> tuple[int, list[str]]:
    """Separate today's arrivals from source-level rejects."""
    with (
        tempfile.TemporaryDirectory(prefix="twitter-parts-") as td,
        Path(td, "day.csv").open("w", encoding="utf-8", newline="") as daily_file,
        Path(td, "rejects.csv").open("w", encoding="utf-8", newline="") as reject_file,
    ):
        daily = _CsvParts(s3, f"daily/date={day}", daily_file, part_rows)
        rejects = _CsvParts(s3, quarantine_prefix, reject_file, part_rows)
        scanned = 0
        for row in iter_normalized_rows(archive):
            scanned += 1
            parsed = source_day(row[2])
            if publish_day and parsed == day:
                daily.append(row)
            elif publish_quarantine and parsed not in DAYS:
                reason = (
                    "invalid_timestamp" if parsed is None else "unsupported_source_day"
                )
                rejects.append([*row, reason])
        daily.flush()
        rejects.flush()
    if scanned != expected_rows or scanned <= 0:
        msg = f"Archive has {scanned} rows, expected {expected_rows}"
        raise ValueError(msg)
    if publish_quarantine:
        json_write(
            s3,
            "bronze",
            f"{quarantine_prefix}/manifest.json",
            {
                "scanned_count": scanned,
                "invalid_count": rejects.count,
                "parts": rejects.keys,
            },
        )
    return daily.count, daily.keys


def _check_archive_quality(
    s3: Any,
    report: dict[str, Any],
    expected_rows: int,
    max_bad_fraction: float,
) -> None:
    """Validate quarantine counts, referenced objects and the rejection threshold."""
    if report["scanned_count"] != expected_rows or expected_rows <= 0:
        msg = "Source quarantine report has the wrong archive row count"
        raise ValueError(msg)
    invalid = report["invalid_count"]
    if not 0 <= invalid <= expected_rows or bool(invalid) != bool(report["parts"]):
        msg_0 = "Source quarantine report has inconsistent reject counts"
        raise ValueError(msg_0)
    if any(not exists(s3, "bronze", key) for key in report["parts"]):
        msg_1 = "Source quarantine files missing"
        raise ValueError(msg_1)
    if invalid / expected_rows > max_bad_fraction:
        msg_2 = "Unassignable source row ratio above threshold. See source quarantine"
        raise ValueError(msg_2)


def mark_complete(
    s3: Any,
    day: str,
    run_id: str,
    source_version: str,
) -> dict[str, str]:
    """Record a successful day only after the DAG verified this exact Gold release."""
    valid_run_id(run_id)
    if day not in DAYS:
        msg = "Invalid completion date"
        raise ValueError(msg)
    prefix = f"daily/date={day}"
    manifest = json_read(s3, "bronze", f"{prefix}/manifest.json")
    if (
        manifest["source_date_cst"] != day
        or manifest["source_version"] != source_version
    ):
        msg_0 = "Completion does not match the Bronze input"
        raise ValueError(msg_0)
    result = {
        "source_date_cst": day,
        "source_version": source_version,
        "run_id": run_id,
    }
    json_write(s3, "bronze", f"{prefix}/completed.json", result)
    return result


def ingest_day(
    s3: Any = None,
    *,
    day: str | None = None,
    max_bad_fraction: float = MAX_BAD_FRACTION,
) -> dict[str, Any]:
    """Prepare one requested or unfinished source day and return its batch metadata."""
    s3 = s3 or client()
    if not 0 <= max_bad_fraction <= 1:
        msg = "Invalid bad-record threshold"
        raise ValueError(msg)
    if day and day not in DAYS:
        msg_0 = f"Unsupported source date {day}. Choose one of {DAYS}"
        raise ValueError(msg_0)
    if day is None:
        day = next(
            (
                d
                for d in DAYS
                if not exists(s3, "bronze", f"daily/date={d}/completed.json")
            ),
            None,
        )
        if day is None:
            return {
                "status": "no_data",
                "reason": "All seven historical days have verified Gold releases",
            }

    key = f"daily/date={day}/manifest.json"
    manifest = json_read(s3, "bronze", key) if exists(s3, "bronze", key) else None
    source = prepare_source(s3)
    archive_sha = source["sha256"]
    if manifest is not None:
        if (
            manifest["source_date_cst"] != day
            or manifest["archive_sha256"] != archive_sha
        ):
            msg_1 = "Committed Bronze manifest does not match the source day/archive"
            raise ValueError(msg_1)
        if not manifest.get("parts") or any(
            not exists(s3, "bronze", k) for k in manifest["parts"]
        ):
            msg_2 = "Committed Bronze data files missing"
            raise ValueError(msg_2)

    quarantine_prefix = f"source/quarantine/{archive_sha}"
    report_key = f"{quarantine_prefix}/manifest.json"
    report_exists = exists(s3, "bronze", report_key)
    if report_exists:
        _check_archive_quality(
            s3, json_read(s3, "bronze", report_key), DATASET_ROWS, max_bad_fraction,
        )
    if manifest is None or not report_exists:
        with tempfile.TemporaryDirectory(prefix="twitter-input-") as td:
            local = Path(td) / "archive.zip"
            s3.download_file("bronze", ARCHIVE_KEY, str(local))
            sha = hashlib.sha256()
            with local.open("rb") as stream:
                for chunk in iter(partial(stream.read, 1024 * 1024), b""):
                    sha.update(chunk)
            if sha.hexdigest() != archive_sha:
                msg_3 = "Cached source archive integrity check failed"
                raise ValueError(msg_3)
            count, keys = _scan_archive(
                s3,
                local,
                day,
                quarantine_prefix,
                DATASET_ROWS,
                publish_day=manifest is None,
                publish_quarantine=not report_exists,
            )
        if not report_exists:
            _check_archive_quality(
                s3, json_read(s3, "bronze", report_key), DATASET_ROWS, max_bad_fraction,
            )
        if manifest is None:
            if count <= 0:
                msg_4 = f"No observations found for source day {day}"
                raise ValueError(msg_4)
            digest = hashlib.sha256((archive_sha + ":" + day).encode()).hexdigest()
            manifest = {
                "schema_version": 2,
                "source_version": "sha256-" + digest[:20],
                "sha256": digest,
                "source_type": "simulated_daily_arrival",
                "source_date_cst": day,
                "archive_sha256": archive_sha,
                "record_count": count,
                "parts": keys,
                "raw_key": ARCHIVE_KEY,
                "source_url": UCI_URL,
                "source_quarantine": report_key,
            }
            json_write(s3, "bronze", key, manifest)
    return {
        "status": "ready",
        "source_version": manifest["source_version"],
        "record_count": manifest["record_count"],
        "source_date_cst": day,
    }


def main() -> None:
    """Run daily ingestion or record a verified completion from CLI arguments."""
    p = argparse.ArgumentParser(description="Simulate one source-day arrival")
    p.add_argument("--date", default="", help="2013-01-12 through 2013-01-18")
    p.add_argument("--max-bad-fraction", type=float, default=MAX_BAD_FRACTION)
    p.add_argument("--complete-run", help="Mark a run after Gold verification")
    p.add_argument("--source-version", help="Bronze version of the verified release")
    args = p.parse_args()
    if args.complete_run:
        result = mark_complete(
            client(), args.date, args.complete_run, args.source_version,
        )
    else:
        result = ingest_day(
            day=args.date or None, max_bad_fraction=args.max_bad_fraction,
        )
    print(json.dumps(result, sort_keys=True))
    if os.environ.get("AIRFLOW_XCOM", "false").lower() == "true":
        out = Path("/airflow/xcom/return.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(result), encoding="utf-8")


if __name__ == "__main__":
    main()
