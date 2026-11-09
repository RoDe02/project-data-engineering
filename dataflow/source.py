"""Bounded, streaming UCI archive parsing, without loading millions of rows in RAM."""

import csv
import gzip
import io
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import TextIO
from zipfile import ZipFile

MAX_ENTRY_BYTES = 8 * 1024**3
MAX_TOTAL_BYTES = 12 * 1024**3
MAX_MEMBERS = 200


def normalize_line(line: str) -> list[str] | None:
    """Emit five CSV fields (4 raw values + rejection hint). Never drop malformed rows."""
    original = line.strip().lstrip("\ufeff")
    if not original or original.startswith("#"):
        return None
    if "," in original:
        try:
            fields = next(csv.reader([original], skipinitialspace=True))
        except csv.Error:
            fields = [original]
    elif "\t" in original:
        fields = original.split("\t")
    else:
        fields = original.split()
    fields = [str(x).strip().strip('"') for x in fields]
    if fields and fields[0].lower() in {"longitude", "lon"}:
        header = [field.lower() for field in fields]
        if (
            len(header) == 4
            and header[1] in {"latitude", "lat"}
            and header[2] == "timestamp"
            and header[3] in {"timezone", "time_zone"}
        ):
            return None
    # Timestamp may be split into date/time if the source is space-delimited.
    if len(fields) >= 4:
        lon, lat = fields[:2]
        zone = fields[-1]
        timestamp = " ".join(fields[2:-1])
        return [lon, lat, timestamp, zone, ""]
    return ["", "", "", "", original]


def _nested_csv(path: Path, depth: int = 0) -> Iterator[TextIO]:
    """Yield text streams from nested zip/gzip/CSV. Reject unexpected binary entries."""
    if depth > 3:
        msg = "ZIP nesting limit exceeded"
        raise ValueError(msg)
    if path.suffix.lower() == ".zip":
        with ZipFile(path) as archive:
            files = [
                v
                for v in archive.infolist()
                if not v.is_dir() and not v.filename.startswith("__MACOSX/")
            ]
            if (
                len(files) > MAX_MEMBERS
                or sum(f.file_size for f in files) > MAX_TOTAL_BYTES
            ):
                msg_0 = "Archive exceeds member/size budget"
                raise ValueError(msg_0)
            found = False
            for f in files:
                if (
                    f.file_size > MAX_ENTRY_BYTES
                    or f.filename.startswith("/")
                    or ".." in Path(f.filename).parts
                ):
                    msg_1 = "Unsafe ZIP member"
                    raise ValueError(msg_1)
                suffix = Path(f.filename).suffix.lower()
                if suffix not in {".zip", ".csv", ".txt", ".dat", ".tsv", ".gz"}:
                    continue
                found = True
                with archive.open(f) as content:
                    if suffix == ".zip":
                        with tempfile.NamedTemporaryFile(suffix=".zip") as nested:
                            shutil.copyfileobj(content, nested, length=1024 * 1024)
                            nested.flush()
                            yield from _nested_csv(Path(nested.name), depth + 1)
                    elif suffix == ".gz":
                        with gzip.GzipFile(fileobj=content) as packed:
                            yield io.TextIOWrapper(
                                packed, encoding="utf-8-sig", errors="replace",
                            )
                    else:
                        yield io.TextIOWrapper(
                            content, encoding="utf-8-sig", errors="replace",
                        )
            if not found:
                msg_2 = "Archive has no supported tabular file"
                raise ValueError(msg_2)
    elif path.suffix.lower() in {".csv", ".txt", ".dat", ".tsv"}:
        with path.open(encoding="utf-8-sig", errors="replace") as f:
            yield f
    else:
        msg_3 = f"Unsupported source file: {path}"
        raise ValueError(msg_3)


def iter_normalized_rows(path: Path) -> Iterator[list[str]]:
    """Stream normalized rows from supported source files and archives."""
    for stream in _nested_csv(path):
        # Iterable streams are closed by outer zip context or caller's file handler.
        for line in stream:
            row = normalize_line(line)
            if row is not None:
                yield row
