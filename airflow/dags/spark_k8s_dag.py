"""
DAG: spark_k8s_dag
Run Spark job on Kubernetes using KubernetesPodOperator (not SparkSubmitOperator).
Use case: isolate heavy Spark job from Airflow worker — resource isolation, separate log stream.
"""
from __future__ import annotations

import pendulum
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=4),
}

with DAG(
    dag_id="spark_k8s_dag",
    description="Spark job on Kubernetes via KubernetesPodOperator",
    default_args=DEFAULT_ARGS,
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["spark", "k8s"],
) as dag:

    spark_etl = KubernetesPodOperator(
        task_id="spark_etl_sales",
        namespace="airflow-jobs",
        image="registry.data-platform.internal/spark:3.5.1-py3.11",
        cmds=["spark-submit"],
        arguments=[
            "--master", "k8s://https://kubernetes.default.svc:443",
            "--conf", "spark.kubernetes.namespace=airflow-jobs",
            "--conf", "spark.kubernetes.container.image=registry.data-platform.internal/spark:3.5.1-py3.11",
            "--conf", "spark.executor.instances=4",
            "--conf", "spark.executor.memory=8g",
            "--conf", "spark.executor.cores=2",
            "--conf", "spark.driver.memory=4g",
            "--conf", "spark.sql.shuffle.partitions=200",
            "--conf", "spark.sql.adaptive.enabled=true",
            "--conf", "spark.sql.adaptive.skewJoin.enabled=true",
            "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
            "/opt/spark/jobs/etl_sales_daily.py",
            "--source", "postgresql://orders-db:5432/orders",
            "--target", "s3a://analytics/iceberg/fact_sales_daily",
        ],
        env_vars=[
            {"name": "AWS_ACCESS_KEY_ID", "value_from": {"secret_key_ref": {"name": "s3-creds", "key": "access_key"}}},
            {"name": "AWS_SECRET_ACCESS_KEY", "value_from": {"secret_key_ref": {"name": "s3-creds", "key": "secret_key"}}},
            {"name": "S3_ENDPOINT", "value": "https://s3.data-platform.internal"},
        ],
        resources={
            "request_memory": "4Gi",
            "request_cpu": "2",
            "limit_memory": "8Gi",
            "limit_cpu": "4",
        },
        volume_mounts=[
            {"name": "spark-config", "mount_path": "/opt/spark/conf"},
        ],
        volumes=[
            {"name": "spark-config", "config_map": {"name": "spark-defaults"}},
        ],
        node_selector={"workload-type": "spark", "pool": "compute"},
        tolerations=[
            {"key": "workload-type", "value": "spark", "effect": "NoSchedule"},
        ],
        affinity={
            "podAntiAffinity": {
                "preferredDuringSchedulingIgnoredDuringExecution": [
                    {"weight": 100, "podAffinityTerm": {
                        "labelSelector": {"matchLabels": {"app": "spark-executor"}},
                        "topologyKey": "kubernetes.io/hostname",
                    }}
                ]
            }
        },
        labels={"app": "spark-etl", "team": "data-platform"},
        get_logs=True,
        log_events_on_failure=True,
        do_xcom_push=True,                  # push driver stdout → XCom
        is_delete_operator_pod=True,        # cleanup after completion
        retries=2,
    )
