"""
DAG: sales_daily_dag
Daily ETL pipeline: extract PostgreSQL orders → transform in dbt → load into Iceberg on S3
Demonstrates: TaskFlow API, SLA, XCom, pools, retries, depends_on_past.
"""
from __future__ import annotations

import pendulum
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.task_group import TaskGroup

from hooks.sqla_hook import SQLAlchemyAsyncHook
from operators.dq_checkpoint_operator import GreatExpectationsCheckpointOperator


DOC_MD = """
### Sales Daily Pipeline

**Owner:** data-platform@platform.example.com
**SLA:** 02:00 UTC (4 hours from midnight)
**Source:** PostgreSQL `orders`, `customers`
**Target:** Apache Iceberg `analytics.fact_sales_daily` on S3

#### Dependencies
- Upstream: `pg_orders_extract` must finish by 01:00 UTC
- Downstream: `mart_customer_rfm` dbt model depends on `fact_sales_daily`

#### Recovery
- Idempotent: re-run safe (MERGE INTO on (order_id, event_date))
- Backfill: `airflow dags backfill -s 2026-11-01 -e 2026-11-20 sales_daily_dag`
"""

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["data-platform-alerts@platform.example.com"],
    "execution_timeout": timedelta(hours=2),
}


@dag(
    dag_id="sales_daily_dag",
    description="Daily sales ETL: PG → dbt staging → Iceberg → marts",
    default_args=DEFAULT_ARGS,
    schedule="0 0 * * *",                   # midnight UTC
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=lambda dag, task_list, blocking_task_list: (
        __import__("logging").getLogger(__name__).error(
            "SLA missed for dag=%s tasks=%s", dag.dag_id, [t.task_id for t in task_list]
        )
    ),
    tags=["sales", "iceberg", "daily", "retail"],
    doc_md=DOC_MD,
    pool="etl_pool",                        # pool with 4 slots — protects source DB
)
def sales_daily_dag() -> None:

    @task(
        task_id="extract_orders",
        pool="pg_source_pool",              # separate pool for source DB protection
        sla=pendulum.duration(hours=1),     # SLA: must complete within 1 hour
    )
    def extract_orders(**context) -> dict:
        """Extract yesterday\'s orders from PostgreSQL."""
        from datetime import datetime, timezone
        logical_date = context["logical_date"]
        start = logical_date.to_iso8601_string()
        end = (logical_date.add(days=1)).to_iso8601_string()

        hook = SQLAlchemyAsyncHook(postgres_conn_id="pg_orders")
        rows = hook.extract(
            sql="""
                SELECT order_id, customer_id, product_id, quantity, unit_price,
                       order_ts, status, store_id
                FROM   orders
                WHERE  order_ts >= %(start)s AND order_ts < %(end)s
            """,
            parameters={"start": start, "end": end},
        )
        if not rows:
            raise AirflowFailException(f"No orders found between {start} and {end}")
        return {"rows_count": len(rows), "start": start, "end": end}

    @task(task_id="transform_staging")
    def transform_staging(extract_meta: dict) -> dict:
        """Trigger dbt build for staging models (Slim CI: state:modified+)."""
        from subprocess import run, CalledProcessError
        try:
            run(
                ["dbt", "build", "--select", "staging", "--target", "prod",
                 "--vars", f'{{"start_date": "{extract_meta["start"]}", "end_date": "{extract_meta["end"]}"}}'],
                check=True, cwd="/opt/airflow/dbt",
            )
        except CalledProcessError as e:
            raise AirflowFailException(f"dbt staging build failed: {e}")
        return {**extract_meta, "dbt_status": "ok"}

    @task(task_id="load_iceberg")
    def load_iceberg(meta: dict) -> dict:
        """Run Spark job to MERGE INTO Iceberg table analytics.fact_sales_daily."""
        from subprocess import run, CalledProcessError
        try:
            run(
                ["spark-submit",
                 "--master", "k8s://https://kubernetes.default.svc",
                 "--conf", "spark.kubernetes.container.image=registry/data-platform/spark:3.5.1",
                 "/opt/airflow/spark/jobs/write_iceberg.py",
                 "--start", meta["start"], "--end", meta["end"]],
                check=True,
            )
        except CalledProcessError as e:
            raise AirflowFailException(f"Iceberg MERGE failed: {e}")
        return {**meta, "iceberg_status": "ok"}

    with TaskGroup("dq_group") as dq_group:
        dq_orders = GreatExpectationsCheckpointOperator(
            task_id="dq_orders",
            checkpoint_name="orders_checkpoint",
            suite_name="orders_suite",
            conn_id="pg_orders",
            validation_failure_raise=True,   # fail-fast — blocks downstream
        )
        dq_fact = GreatExpectationsCheckpointOperator(
            task_id="dq_fact_sales_daily",
            checkpoint_name="fact_sales_daily_checkpoint",
            suite_name="fact_sales_daily_suite",
            conn_id="iceberg_trino",
        )

    @task(task_id="build_marts")
    def build_marts(meta: dict) -> dict:
        from subprocess import run, CalledProcessError
        run(["dbt", "build", "--select", "marts", "--target", "prod"],
            check=True, cwd="/opt/airflow/dbt")
        return {**meta, "marts_status": "ok"}

    @task(task_id="publish_metric_layer")
    def publish_metric_layer(meta: dict) -> dict:
        """Refresh Cube-core pre-aggregations."""
        from subprocess import run
        run(["cubejs", "refresh", "--schema", "/opt/airflow/semantic_layer/cube/schemas"],
            check=True)
        return {**meta, "cube_status": "ok"}

    @task(task_id="notify_done")
    def notify_done(meta: dict) -> None:
        from airflow.utils.email import send_email
        send_email(
            to="data-platform-alerts@platform.example.com",
            subject=f"[OK] sales_daily_dag completed — {meta['rows_count']} rows",
            html_content=f"<p>rows: {meta['rows_count']}</p><p>iceberg: ok</p><p>marts: ok</p>",
        )

    done = EmptyOperator(task_id="done")

    extract = extract_orders()
    staging = transform_staging(extract)
    iceberg = load_iceberg(staging)
    dq_group.set_upstream(iceberg)
    marts = build_marts(iceberg)
    cube = publish_metric_layer(marts)
    notify_done(cube) >> done


sales_daily_dag()
