"""Spark S3A and release helpers. Imported only within Spark driver."""

import argparse
import os

from pyspark.sql import SparkSession

from .daily import DAYS
from .settings import MAX_BAD_FRACTION, valid_run_id


def arguments() -> argparse.Namespace:
    """Parse and validate the shared Spark batch arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-version", required=True)
    parser.add_argument("--source-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--max-bad-fraction", type=float, default=MAX_BAD_FRACTION)
    args = parser.parse_args()
    args.run_id = valid_run_id(args.run_id)

    if args.source_date not in DAYS:
        msg = "Unsupported source date"
        raise ValueError(msg)
    if not 0 <= args.max_bad_fraction <= 1:
        msg_0 = "Invalid bad-record threshold"
        raise ValueError(msg_0)
    if not args.source_version.startswith("sha256-"):
        msg_1 = "Unrecognized source version"
        raise ValueError(msg_1)
    return args


def spark_session(name: str) -> SparkSession:
    """Create a UTC Spark session configured for the MinIO S3 endpoint."""
    spark = (
        SparkSession.builder.appName(name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    hc = spark.sparkContext._jsc.hadoopConfiguration()
    hc.set("fs.s3a.endpoint", os.environ["S3_ENDPOINT"])
    hc.set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    hc.set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    hc.set("fs.s3a.path.style.access", "true")
    hc.set(
        "fs.s3a.connection.ssl.enabled",
        "true" if os.getenv("S3_VERIFY_TLS") == "true" else "false",
    )
    hc.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hc.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    # Disable slow/unsafe object-store rename assumptions, manifests mark successful publications.
    return spark
