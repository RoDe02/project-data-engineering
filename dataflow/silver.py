"""Bronze CSV -> validated Silver Parquet and quarantined rejects."""

from datetime import datetime, timezone
from time import monotonic

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dataflow.spark_common import arguments, spark_session
from dataflow.store import client, exists, json_read, json_write
from dataflow.verify import has_parquet, parquet_objects

RAW_FIELDS = (
    "longitude_raw",
    "latitude_raw",
    "timestamp_raw",
    "timezone_raw",
    "bad_line",
)
SOURCE_SCHEMA = ", ".join(f"{name} STRING" for name in RAW_FIELDS)


def transform(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split raw rows into standardized records and rejected input rows."""
    lon = F.col("longitude_raw").cast("double")
    lat = F.col("latitude_raw").cast("double")
    zone = F.col("timezone_raw").cast("int")
    raw_date = F.coalesce(
        F.to_timestamp("timestamp_raw", "yyyyMMddHHmmss"),
        F.to_timestamp("timestamp_raw", "yyyy-MM-dd HH:mm:ss"),
    )
    # Source times are CST (UTC-6), not the location's zone field.
    utc = F.to_utc_timestamp(raw_date, "America/Chicago")
    staged = (
        raw.withColumn("longitude", lon)
        .withColumn("latitude", lat)
        .withColumn("timezone_code", zone)
        .withColumn("event_time_utc", utc)
    )
    # Spark CSV reads an empty fifth field as NULL by default. Both NULL
    # and the empty string mean the ingester reported no malformed line.
    no_parse_error = F.coalesce(F.col("bad_line"), F.lit("")) == ""
    valid_conditions = (
        no_parse_error
        & lon.between(-180.0, 180.0)
        & lat.between(-90.0, 90.0)
        & F.col("event_time_utc").isNotNull()
        & zone.isin(1, 2, 3, 4)
        & (F.col("timezone_raw") == F.col("timezone_code").cast("string"))
    )
    # Treat every other NULL validation result as invalid, not UNKNOWN.
    valid = F.coalesce(valid_conditions, F.lit(False))
    good = (
        staged.filter(valid)
        .withColumn("event_date", F.to_date("event_time_utc"))
        .select(
            "longitude", "latitude", "event_time_utc", "event_date", "timezone_code",
        )
    )
    rejected = staged.filter(~valid).select(*RAW_FIELDS)
    return good, rejected


def main() -> None:
    """Validate a Bronze batch and publish Silver data with its manifest."""
    args = arguments()
    s3 = client()
    source = json_read(s3, "bronze", f"daily/date={args.source_date}/manifest.json")
    if source["source_version"] != args.source_version:
        msg = "Source manifest version mismatch"
        raise ValueError(msg)
    prefix = f"daily/date={args.source_date}/runs/{args.run_id}"
    root = f"s3a://silver/{prefix}"
    manifest_key = f"{prefix}/manifest.json"
    if exists(s3, "silver", manifest_key):
        prior = json_read(s3, "silver", manifest_key)
        if (
            prior["source_version"] != args.source_version
            or prior["source_count"] != source["record_count"]
        ):
            msg_0 = "Cannot overwrite a committed Silver run with different inputs"
            raise ValueError(msg_0)
        if not has_parquet(s3, "silver", f"{prefix}/records/"):
            msg_1 = "Committed Silver run has no Parquet data"
            raise ValueError(msg_1)
        if prior["invalid_count"] and not has_parquet(
            s3, "silver", f"{prefix}/quarantine/",
        ):
            msg_2 = "Committed Silver run has no quarantine data"
            raise ValueError(msg_2)
        return
    started = monotonic()
    spark = spark_session("twitter-silver-" + args.run_id)
    spark_conf = spark.sparkContext.getConf()
    write_partitions = int(spark_conf.get("spark.twitter.silver.writePartitions", "64"))
    if write_partitions < 1:
        msg_3 = "Silver write partition count must be positive"
        raise ValueError(msg_3)
    # Explicit input filenames prevent accidental inclusion of uncommitted staging data.
    paths = [f"s3a://bronze/{key}" for key in source["parts"]]
    raw = spark.read.option("header", "false").schema(SOURCE_SCHEMA).csv(paths)
    good, rejected = transform(raw)
    good.persist()
    rejected.persist()
    ok = good.count()
    bad = rejected.count()
    expected = source["record_count"]
    if bad:
        rejected.write.mode("overwrite").parquet(f"{root}/quarantine")
    if ok + bad != expected or expected <= 0:
        msg_4 = f"Record conservation failed: valid={ok} bad={bad} expected={expected}"
        raise ValueError(msg_4)
    if bad / expected > args.max_bad_fraction:
        msg_5 = f"Invalid record ratio {bad / expected:.5f} above threshold"
        raise ValueError(msg_5)
    (
        good.repartition(write_partitions)
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{root}/records")
    )
    sizes = [obj["Size"] for obj in parquet_objects(s3, "silver", f"{prefix}/records/")]
    if not sizes:
        msg_6 = "Silver write produced no Parquet files"
        raise ValueError(msg_6)
    manifest = {
        "schema_version": 1,
        "run_id": args.run_id,
        "source_version": args.source_version,
        "source_date_cst": args.source_date,
        "source_count": expected,
        "valid_count": ok,
        "invalid_count": bad,
        "write_metrics": {
            "executor_instances": spark_conf.get("spark.executor.instances"),
            "write_partitions": write_partitions,
            "parquet_files": len(sizes),
            "bytes": sum(sizes),
            "min_file_bytes": min(sizes),
            "max_file_bytes": max(sizes),
            "elapsed_seconds": round(monotonic() - started, 3),
        },
        "record_path": f"{root}/records",
        "quarantine_path": f"{root}/quarantine" if bad else None,
        "published_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    # Readiness marker is only created after Spark output + quality gates complete.
    json_write(s3, "silver", manifest_key, manifest)
    good.unpersist()
    rejected.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
