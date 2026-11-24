"""
Spark 3.5 — write to Apache Iceberg 1.4 table with MERGE INTO and time-travel.
Demonstrates: Iceberg v1/v2, MERGE INTO, partition evolution, schema evolution,
snapshot isolation, expire snapshots.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("write_iceberg")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.analytics", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.analytics.type", "hadoop")
        .config("spark.sql.catalog.analytics.warehouse", "s3a://analytics/iceberg")
        .config("spark.sql.catalog.analytics.s3.endpoint", "https://s3.data-platform.internal")
        .config("spark.sql.catalog.analytics.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", "analytics")
        .getOrCreate()
    )


def upsert_iceberg(spark: SparkSession, source_path: str, table: str) -> None:
    """MERGE INTO Iceberg table — idempotent upsert by primary key."""
    df = spark.read.parquet(source_path)

    spark.sql(f"""
        MERGE INTO analytics.{table} AS t
        USING (
            SELECT order_date, store_id, product_id,
                   sum(orders_count) AS orders_count,
                   sum(quantity_total) AS quantity_total,
                   sum(revenue_total) AS revenue_total
            FROM (SELECT * FROM parquet.`{source_path}`)
            GROUP BY 1, 2, 3
        ) AS s
        ON  t.order_date = s.order_date
        AND t.store_id   = s.store_id
        AND t.product_id = s.product_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Print table history (time travel)
    history = spark.sql(f"SELECT snapshot_id, committed_at, operation, summary FROM analytics.{table}.history")
    history.show(truncate=False)


def time_travel_read(spark: SparkSession, table: str) -> None:
    """Read table as of a previous snapshot (audit / rollback)."""
    snapshots = spark.sql(f"SELECT snapshot_id, committed_at FROM analytics.{table}.history ORDER BY committed_at DESC LIMIT 5")
    snapshots.show()

    latest_snapshot = snapshots.first()["snapshot_id"]
    log.info("Reading table as of snapshot %s", latest_snapshot)
    df_old = spark.sql(f"SELECT * FROM analytics.{table}.VERSION AS OF {latest_snapshot}")
    log.info("Rows at latest snapshot: %d", df_old.count())


def expire_snapshots(spark: SparkSession, table: str, retain_days: int = 7) -> None:
    """Expire old snapshots to control storage growth."""
    spark.sql(f"CALL analytics.system.expire_snapshots(table => 'analytics.{table}', older_than => TIMESTAMP '{(datetime.utcnow()).strftime("%Y-%m-%d %H:%M:%S")}', retain_last => 5)")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--table", default="fact_sales_daily")
    p.add_argument("--retain-days", type=int, default=7)
    args = p.parse_args()

    spark = build_spark()
    try:
        upsert_iceberg(spark, args.source, args.table)
        time_travel_read(spark, args.table)
        expire_snapshots(spark, args.table, args.retain_days)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
