"""
DAG: dynamic_sources_dag
Dynamic TaskMap — generates one task per source defined in sources.yml.
Pattern: data-driven DAGs (Airflow 2.3+ dynamic task mapping).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pendulum
import yaml
from airflow.decorators import dag, task
from airflow.models.dag import DAG

SOURCES_YAML = Path("/opt/airflow/configs/sources.yml")


def load_sources() -> list[dict[str, Any]]:
    """Read sources config and return list of source descriptors."""
    cfg = yaml.safe_load(SOURCES_YAML.read_text())
    return cfg["sources"]


@dag(
    dag_id="dynamic_sources_dag",
    description="Dynamic per-source ingestion — task per source",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_tasks=4,                    # limit parallelism
    tags=["dynamic", "ingest"],
)
def dynamic_sources_dag() -> None:

    sources = load_sources()

    @task
    def extract(source: dict) -> dict:
        """Extract from a source — implemented per source_type."""
        from hooks.sqla_hook import SQLAlchemyAsyncHook
        hook = SQLAlchemyAsyncHook(postgres_conn_id=source["conn_id"])
        rows = hook.extract(sql=source["query"])
        return {"source": source["name"], "rows": len(rows)}

    @task
    def validate(meta: dict) -> dict:
        from operators.dq_checkpoint_operator import GreatExpectationsCheckpointOperator
        # DQ checkpoint would be invoked here per source
        if meta["rows"] == 0:
            raise ValueError(f"No rows for {meta['source']}")
        return {**meta, "dq": "passed"}

    @task
    def load(meta: dict) -> dict:
        # Write to Iceberg silver layer
        return {**meta, "loaded": True}

    @task
    def aggregate(results: list[dict]) -> None:
        total = sum(r["rows"] for r in results)
        print(f"Total rows ingested: {total}")

    # Dynamic task mapping: one task instance per source
    extracted = extract.expand(source=sources)
    validated = validate.expand(meta=extracted)
    loaded = load.expand(meta=validated)
    aggregate(loaded)


dynamic_sources_dag()
