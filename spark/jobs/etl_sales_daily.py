"""
Spark 3.5 / PySpark — daily sales ETL with optimizations:
- broadcast-join for small dim tables
- salting for data skew on customer_id
- adaptive query execution (AQE)
- checkpoint for iterative workloads
- zstd compression on output Parquet
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, NumericType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def build_spark() -> SparkSession:
    """Build SparkSession with tuned configs for YARN / k8s."""
    return (
        SparkSession.builder
        .appName("etl_sales_daily")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.sql.parquet.rowGroupByLength", "134217728")     # 128 MB row group
        .config("spark.sql.broadcastTimeout", "600")
        .config("spark.sql.autoBroadcastJoinThreshold", "104857600")   # 100 MB broadcast
        .config("spark.sql.files.maxPartitionBytes", "134217728")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "https://s3.data-platform.internal")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .getOrCreate()
    )


def resolve_skew(df: DataFrame, key: str, salt_bins: int = 16) -> DataFrame:
    """Salting technique — break heavy-skewed key into N buckets to balance partitions."""
    return (
        df.withColumn("__salt", (F.rand() * salt_bins).cast("int"))
          .withColumn(f"{key}_salted", F.concat(F.col(key).cast("string"), F.lit("_"), F.col("__salt")))
    )


def etl(spark: SparkSession, pg_url: str, s3_target: str, start: str, end: str) -> int:
    """Read PG → transform → write Parquet to S3 (zstd, partitioned by order_date)."""
    orders = (
        spark.read.format("jdbc")
        .option("url", pg_url)
        .option("dbtable", "(SELECT * FROM orders WHERE order_ts >= '{}' AND order_ts < '{}') t".format(start, end))
        .option("partitionColumn", "order_id")
        .option("numPartitions", "32")
        .option("lowerBound", "0")
        .option("upperBound", "1000000000")
        .option("fetchsize", "10000")
        .load()
    )

    # Read small dim — broadcast
    products = spark.read.parquet("s3a://analytics/dim/product").cache()
    products.count()  # materialize cache

    # Skew handling: orders by customer_id can be heavily skewed (whales)
    orders_salted = resolve_skew(orders, "customer_id", salt_bins=32)

    joined = orders_salted.join(
        F.broadcast(products), on="product_id", how="left"
    )

    result = (
        joined.groupBy("order_date", "store_id", "product_id", "customer_id_salted")
              .agg(
                  F.countDistinct("order_id").alias("orders_count"),
                  F.sum("quantity").alias("quantity_total"),
                  F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue_total"),
              )
              .drop("customer_id_salted", "__salt")
    )

    log.info("Writing %d rows to %s", result.count(), s3_target)
    (
        result.write
        .mode("append")
        .format("parquet")
        .option("compression", "zstd")
        .partitionBy("order_date")
        .save(s3_target)
    )

    return result.count()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True, help="PostgreSQL JDBC URL")
    p.add_argument("--target", required=True, help="S3 target path")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    args = p.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        rows = etl(spark, args.source, args.target, args.start, args.end)
        log.info("DONE: %d rows written to %s", rows, args.target)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
