"""Manually triggered daily-arrival simulator over the historic UCI archive."""

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.operators.python import ShortCircuitOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from kubernetes.client import (
    V1Capabilities,
    V1ConfigMapEnvSource,
    V1EnvFromSource,
    V1ResourceRequirements,
    V1SecretEnvSource,
    V1SeccompProfile,
    V1SecurityContext,
)

from airflow import DAG

NAMESPACE = os.environ.get("PLATFORM_NAMESPACE", "twitter-data")
INGEST_IMAGE = os.environ.get("INGEST_IMAGE", "twitter-ingest:local")
SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "twitter-spark:local")
RUN_ID = "{{ dag_run.run_id | lower | replace(':', '-') | replace('+', '-') | replace('/', '-') | replace('_', '-') }}"
SOURCE = "{{ ti.xcom_pull(task_ids='ingest')['source_version'] }}"
SOURCE_DATE = "{{ ti.xcom_pull(task_ids='ingest')['source_date_cst'] }}"


def k8s_env(identity: str) -> list[V1EnvFromSource]:
    """Build environment references for shared settings and an identity secret."""
    return [
        V1EnvFromSource(config_map_ref=V1ConfigMapEnvSource(name="platform-settings")),
        V1EnvFromSource(secret_ref=V1SecretEnvSource(name=identity)),
    ]

def ingestion_security_context() -> V1SecurityContext:
    """Run ingestion and verification containers without elevated privileges."""
    return V1SecurityContext(
        run_as_non_root=True,
        run_as_user=10001,
        allow_privilege_escalation=False,
        capabilities=V1Capabilities(drop=["ALL"]),
        seccomp_profile=V1SeccompProfile(type="RuntimeDefault"),
    )


def spark_setting(name: str, default: int) -> int:
    """Read a positive integer Spark setting from the environment."""
    value = int(os.environ.get(f"TWITTER_SPARK_{name}", default))
    if value < 1:
        msg = f"TWITTER_SPARK_{name} must be a positive integer"
        raise ValueError(msg)
    return value


def spark_template(stage: str) -> dict[str, Any]:
    """Build a SparkApplication specification for a processing stage."""
    driver_cores = spark_setting("DRIVER_CORES", 1)
    executor_cores = spark_setting("EXECUTOR_CORES", 1)
    processing_env = [
        {"configMapRef": {"name": "platform-settings"}},
        {"secretRef": {"name": "minio-processing"}},
    ]
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": f"twitter-{stage}-{RUN_ID}-t{{{{ ti.try_number }}}}",
            "namespace": NAMESPACE,
            "labels": {"app.kubernetes.io/part-of": "twitter-batch"},
        },
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "sparkVersion": "3.5.5",
            "image": SPARK_IMAGE,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": f"local:///opt/app/dataflow/{stage}.py",
            "arguments": [
                "--run-id",
                RUN_ID,
                "--source-version",
                SOURCE,
                "--source-date",
                SOURCE_DATE,
            ],
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": 3600,
            "sparkConf": {
                "spark.sql.session.timeZone": "UTC",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.shuffle.partitions": str(
                    spark_setting("SHUFFLE_PARTITIONS", 64),
                ),
                "spark.twitter.silver.writePartitions": str(
                    spark_setting("SILVER_WRITE_PARTITIONS", 64),
                ),
                "spark.kubernetes.executor.deleteOnTermination": "true",
                "spark.hadoop.fs.s3a.path.style.access": "true",
            },
            "driver": {
                "cores": driver_cores,
                "coreLimit": f"{1000 * driver_cores + 200}m",
                "memory": f"{spark_setting('DRIVER_MEMORY_MIB', 2048)}m",
                "serviceAccount": "spark-runner",
                "labels": {"app": "twitter-spark"},
                "envFrom": processing_env,
                "securityContext": {
                    "runAsNonRoot": True,
                    "runAsUser": 185,
                    "allowPrivilegeEscalation": False,
                    "capabilities": {"drop": ["ALL"]},
                    "seccompProfile": {"type": "RuntimeDefault"},
                },
            },
            "executor": {
                "cores": executor_cores,
                "coreLimit": f"{1000 * executor_cores + 200}m",
                "instances": spark_setting("EXECUTOR_INSTANCES", 2),
                "memory": f"{spark_setting('EXECUTOR_MEMORY_MIB', 2048)}m",
                "memoryOverhead": "512m",
                "labels": {"app": "twitter-spark"},
                "envFrom": processing_env,
                "securityContext": {
                    "runAsNonRoot": True,
                    "runAsUser": 185,
                    "allowPrivilegeEscalation": False,
                    "capabilities": {"drop": ["ALL"]},
                    "seccompProfile": {"type": "RuntimeDefault"},
                },
            },
        },
    }


def has_ready_data(**context: Any) -> bool:
    """Return whether ingestion prepared a day for downstream processing."""
    return (context["ti"].xcom_pull(task_ids="ingest") or {}).get("status") == "ready"


with DAG(
    dag_id="twitter_geospatial_daily_batch",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,  # Manual trigger simulates each daily ingestion event.
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=45),
    },
    dagrun_timeout=timedelta(hours=8),
    tags=["batch", "minio", "spark", "uci", "medallion"],
    doc_md="""Each manual run simulates arrival of one UCI source day (Jan 12-18 2013, CST).
    A default trigger retries the oldest source day without a verified Gold completion.""",
) as dag:
    ingest = KubernetesPodOperator(
        task_id="ingest",
        name="uci-ingest",
        namespace=NAMESPACE,
        image=INGEST_IMAGE,
        cmds=["python", "-m", "dataflow.daily"],
        # Optional source_date in DAG configuration, otherwise select the oldest unfinished day.
        arguments=["--date", "{{ (dag_run.conf or {}).get('source_date', '') }}"],
        service_account_name="data-ingest",
        container_security_context=ingestion_security_context(),
        in_cluster=True,
        env_from=k8s_env("minio-ingestion"),
        env_vars={"AIRFLOW_XCOM": "true"},
        do_xcom_push=True,
        get_logs=True,
        is_delete_operator_pod=True,
        reattach_on_restart=True,
        startup_timeout_seconds=600,
        container_resources=V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "512Mi"},
            limits={"cpu": "2", "memory": "2Gi"},
        ),
    )
    # Skip downstream tasks when no historical day remains.
    has_data = ShortCircuitOperator(
        task_id="has_data",
        python_callable=has_ready_data,
    )
    silver = SparkKubernetesOperator(
        task_id="silver",
        namespace=NAMESPACE,
        in_cluster=True,
        template_spec=spark_template("silver"),
        get_logs=True,
        delete_on_termination=False,
        reattach_on_restart=True,
        startup_timeout_seconds=1200,
    )
    gold = SparkKubernetesOperator(
        task_id="gold",
        namespace=NAMESPACE,
        in_cluster=True,
        template_spec=spark_template("gold"),
        get_logs=True,
        delete_on_termination=False,
        reattach_on_restart=True,
        startup_timeout_seconds=1200,
    )
    validate_release = KubernetesPodOperator(
        task_id="validate_release",
        name="gold-release-check",
        namespace=NAMESPACE,
        image=INGEST_IMAGE,
        cmds=["python", "-m", "dataflow.verify"],
        arguments=[
            "--run-id",
            RUN_ID,
            "--source-date",
            SOURCE_DATE,
            "--source-version",
            SOURCE,
        ],
        service_account_name="data-ingest",
        container_security_context=ingestion_security_context(),
        in_cluster=True,
        env_from=k8s_env("minio-ml-reader"),
        get_logs=True,
        is_delete_operator_pod=True,
        container_resources=V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "128Mi"},
            limits={"cpu": "500m", "memory": "512Mi"},
        ),
    )
    mark_complete = KubernetesPodOperator(
        task_id="mark_complete",
        name="mark-day-complete",
        namespace=NAMESPACE,
        image=INGEST_IMAGE,
        cmds=["python", "-m", "dataflow.daily"],
        arguments=[
            "--date",
            SOURCE_DATE,
            "--complete-run",
            RUN_ID,
            "--source-version",
            SOURCE,
        ],
        service_account_name="data-ingest",
        container_security_context=ingestion_security_context(),
        in_cluster=True,
        env_from=k8s_env("minio-ingestion"),
        get_logs=True,
        is_delete_operator_pod=True,
        container_resources=V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "128Mi"},
            limits={"cpu": "500m", "memory": "512Mi"},
        ),
    )
    ingest >> has_data >> silver >> gold >> validate_release >> mark_complete
