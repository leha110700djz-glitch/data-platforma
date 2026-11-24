# Storage Decision: Parquet vs Iceberg vs Delta

## Контекст

Data platform хранит:
- **bronze (raw)** — данные из источников в исходном виде (PG, Kafka, files)
- **silver (staging/intermediate)** — очищенные данные, dbt-модели
- **gold (marts)** — агрегаты для BI / ML / SemantiLayer

Объёмы: 10+ ТБ/сутки поступающих данных, ~5 ПБ за 18 месяцев (исторический контур).

## Решение

| Слой | Формат | Обоснование |
|------|--------|-------------|
| bronze | **Parquet 1.13** (zstd) | immutable cold-слой; не нужен time travel; min cost; predicate pushdown достаточен |
| silver | **Apache Iceberg 1.4** | нужна schema evolution (источники часто меняют колонки), time travel для аудита, MERGE INTO для идемпотентных upsert, partition evolution (с month → с day по мере роста) |
| gold (mart'ы) | **Iceberg** для changed-витрин (fact_sales_daily — incremental MERGE) + **ClickHouse 23** для горячих OLAP-витрин под BI (range scan в 100× быстрее Iceberg) |

## Почему не Delta Lake?

- Apache Iceberg — нативная интеграция с Spark 3.5 (через catalog), Trino 428 (native reader), Flink, Pig, Hive 3.2 — без vendor lock-in
- Delta Lake требует DBR Runtime или Delta Standalone — extra jar в classpath
- Iceberg community развивается быстрее (Netflix → ASF graduation 2024)

## Почему не Hive+ORC?

- ORC не поддерживает ACID MERGE в открытой форме (Hive 3 ACID сложен)
- Iceberg даёт snapshot isolation из коробки, без compaction-процессов
