"""Silver -> aggregated Gold features with release pointer."""

import os
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dataflow.spark_common import arguments, spark_session
from dataflow.store import client, exists, json_read, json_write
from dataflow.verify import require_identity, verify_features


def aggregate(df: DataFrame) -> DataFrame:
    """Count tweets by UTC hour, timezone code and quarter-degree grid cell."""
    # Quarter-degree grid prevents publishing exact GPS in the ML feature table.
    return (
        df.withColumn("hour_utc", F.date_trunc("hour", "event_time_utc"))
        .withColumn("lat_cell", F.floor(F.col("latitude") * 4) / 4)
        .withColumn("lon_cell", F.floor(F.col("longitude") * 4) / 4)
        .groupBy("hour_utc", "timezone_code", "lat_cell", "lon_cell")
        .agg(F.count(F.lit(1)).alias("tweet_count"))
        .withColumn("event_date", F.to_date("hour_utc"))
    )


def promote_release(s3: Any, release: dict[str, Any]) -> None:
    """Publish a verified release under the DAG's single-writer policy."""
    key = "releases/latest.json"
    if exists(s3, "gold", key):
        latest = json_read(s3, "gold", key)
        if latest == release:
            return
        if (
            latest.get("created_at_utc") and release.get("created_at_utc") and
            datetime.fromisoformat(latest["created_at_utc"]) > datetime.fromisoformat(release["created_at_utc"])
        ):
            msg = "A newer Gold release is already latest. Replay the day in a new run"
            raise ValueError(msg)
    json_write(s3, "gold", key, release)


def main() -> None:
    """Aggregate Silver records, validate Gold output and promote the release."""
    args = arguments()
    s3 = client()
    silver = json_read(
        s3, "silver", f"daily/date={args.source_date}/runs/{args.run_id}/manifest.json",
    )
    require_identity(silver, args.run_id, args.source_date, args.source_version)
    release_key = f"daily/date={args.source_date}/runs/{args.run_id}/release.json"
    if exists(s3, "gold", release_key):
        previous = json_read(s3, "gold", release_key)
        require_identity(previous, args.run_id, args.source_date, args.source_version)
        verify_features(s3, previous)
        # A prior attempt may have committed the release but failed the pointer PUT.
        promote_release(s3, previous)
        return
    record_path = (
        f"s3a://silver/daily/date={args.source_date}/runs/{args.run_id}/records"
    )
    # Earlier daily manifests omitted the date prefix.
    legacy_path = f"s3a://silver/runs/{args.run_id}/records"
    if silver["record_path"] not in (record_path, legacy_path):
        msg = "Silver record path does not match the requested batch"
        raise ValueError(msg)
    spark = spark_session("twitter-gold-" + args.run_id)
    records = spark.read.parquet(record_path)
    all_features = aggregate(records).persist()
    source_sum = (
        all_features.select(F.sum("tweet_count").alias("count")).first()["count"] or 0
    )
    if source_sum != silver["valid_count"]:
        msg_0 = f"Aggregation conservation failed: {source_sum} != {silver['valid_count']}"
        raise ValueError(msg_0)
    min_count = int(os.environ.get("MIN_AGG_COUNT", "5"))
    if min_count < 1:
        msg_1 = "MIN_AGG_COUNT must be positive"
        raise ValueError(msg_1)
    # Suppress low-count cells. Publish only coarse counts above a conservative floor.
    features = all_features.filter(F.col("tweet_count") >= min_count).persist()
    feature_rows = features.count()
    published = (
        features.select(F.sum("tweet_count").alias("count")).first()["count"] or 0
    )
    suppressed = source_sum - published
    if feature_rows < 1:
        msg_2 = "All cells suppressed. Increase fixture density or lower MIN_AGG_COUNT for a test"
        raise ValueError(msg_2)
    gold_path = f"s3a://gold/daily/date={args.source_date}/runs/{args.run_id}/features"
    features.write.mode("overwrite").partitionBy("event_date").parquet(gold_path)
    json_read(
        s3, "bronze", f"daily/date={args.source_date}/manifest.json",
    )
    release = {
        "schema_version": 1,
        "run_id": args.run_id,
        "source_version": args.source_version,
        "source_date_cst": args.source_date,
        "feature_path": gold_path,
        "feature_rows": feature_rows,
        "tweet_count_total": published,
        "source_tweet_count": source_sum,
        "suppressed_tweet_count": suppressed,
        "min_cell_count": min_count,
        "grid_degrees": 0.25,
        "time_granularity": "hour_utc",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    verify_features(s3, release)
    json_write(s3, "gold", release_key, release)
    # Single S3 PUT replaces the latest pointer atomically for readers.
    promote_release(s3, release)
    features.unpersist()
    all_features.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
