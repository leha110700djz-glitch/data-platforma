# DB Design — PostgreSQL 15 / ClickHouse 23

## PostgreSQL (оперданные + DWH marts)

### Партиционирование
Все большие таблицы партиционируются по `order_date` (RANGE):

```sql
CREATE TABLE fact_sales_daily (
    order_date       DATE    NOT NULL,
    store_id         INT     NOT NULL,
    product_id       BIGINT  NOT NULL,
    orders_count     BIGINT,
    quantity_total   BIGINT,
    revenue_total    NUMERIC(18,2)
) PARTITION BY RANGE (order_date);

-- Monthly partitions, automated by pg_partman
CREATE TABLE fact_sales_daily_2026_11 PARTITION OF fact_sales_daily
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
```

### Индексы
- B-tree: `customer_id`, `order_id` (lookup-by-id)
- BRIN: `order_date` на partitions (10× меньше B-tree, достаточно для range-scan)
- Partial index: `WHERE status = 'completed'` (only relevant rows)

### Шардирование (ClickHouse только)
ClickHouse shard by `cityHash64(customer_id) % 4` — 4 shards × 2 replicas.

### Блокировки и изоляция
- `READ COMMITTED` (default) для OLTP-нагрузки
- `REPEATABLE READ` для длинных аналитических транзакций
- `pg_locks` мониторится в Grafana; deadlock-логи идут в alertmanager

### Репликация / бэкапы
- Streaming replication (primary + 1 sync replica)
- `pg_dump` ежедневно + WAL archiving в S3 (PITR окно 30 дней)
- Migrations через `pglogical` (zero-downtime major version upgrade 14 → 16)

## ClickHouse (OLAP marts)

- MergeTree engine, `PARTITION BY toYYYYMM(order_date)` + `ORDER BY (store_id, product_id, order_date)`
- Materialized views для hourly-агрегатов
- Distributed table поверх 4 shards
