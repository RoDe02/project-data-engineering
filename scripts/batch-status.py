"""Read the newest manually requested batch from the Airflow 2 metadata database."""

import json
from typing import Any

from airflow.models import DagRun
from airflow.utils.session import create_session


def latest_batch() -> dict[str, Any]:
    """Return the newest manual batch status and its task outcomes."""
    with create_session() as session:
        run = (
            session.query(DagRun)
            .filter_by(dag_id="twitter_geospatial_daily_batch", run_type="manual")
            .order_by(DagRun.id.desc())
            .first()
        )
        if run is None:
            return {"status": "no manual batch requested"}

        tasks = {ti.task_id: ti for ti in run.get_task_instances(session=session)}
        ingest = tasks.get("ingest")
        arrival = (
            ingest.xcom_pull(task_ids="ingest", session=session) if ingest else None
        ) or {}
        states = {name: ti.state for name, ti in tasks.items()}
        return {
            "airflow_run_id": run.run_id,
            "run_state": run.state,
            "requested_date": (run.conf or {}).get("source_date") or "automatic",
            "source_date": arrival.get("source_date_cst"),
            "arrival": arrival.get("status"),
            "release_validation": states.get("validate_release"),
            "day_completion": states.get("mark_complete"),
            "running_tasks": {
                name: ti.hostname for name, ti in tasks.items() if ti.state == "running"
            },
        }


if __name__ == "__main__":
    print(json.dumps(latest_batch(), indent=2))
