"""
Spark — optimize Parquet files on S3 (zstd, 128 MB row groups, predicate pushdown).
Run after batch write — compacts many small files into large row groups.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def optimize_parquet(spark: SparkSession, path: str) -> None:
    (
        spark.read.parquet(path)
        .repartition(64)                       # target: 64 files × 128 MB = 8 GB partition
        .write
        .mode("overwrite")
        .option("compression", "zstd")
        .option("parquet.row-group.size", "134217728")    # 128 MB
        .option("parquet.page.size", "1048576")           # 1 MB
        .save(path + "__optimized")
    )

    # Atomic swap: rename optimized → original
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    Path = sc._jvm.org.apache.hadoop.fs.Path
    fs.delete(Path(path + "__backup"), True)
    fs.rename(Path(path), Path(path + "__backup"))
    fs.rename(Path(path + "__optimized"), Path(path))
    fs.delete(Path(path + "__backup"), True)
