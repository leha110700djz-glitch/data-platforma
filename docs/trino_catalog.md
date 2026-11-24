# Trino 428 — Federation Queries

## Use Case

Ad-hoc аналитика по Iceberg на S3 + federation к PostgreSQL + ClickHouse.
Trino используется как query-engine для BI-инструментов (Superset / Metabase).

## Catalogs

### `iceberg.properties`
```properties
connector.name=iceberg
iceberg.catalog.type=hadoop
hive.metastore=thrift
hive.metastore.uri=thrift://hms.data-platform.internal:9083
hive.s3.endpoint=https://s3.data-platform.internal
hive.s3.path-style-access=true
hive.s3.ssl.enabled=true
```

### `postgresql.properties`
```properties
connector.name=postgresql
connection-url=jdbc:postgresql://orders-db:5432/orders
connection-user=${ENV:PG_USER}
connection-password=${ENV:PG_PASSWORD}
```

### `clickhouse.properties`
```properties
connector.name=clickhouse
connection-url=jdbc:clickhouse://ch-1:8123,clickhouse://ch-2:8123
connection-user=${ENV:CH_USER}
connection-password=${ENV:CH_PASSWORD}
```

## Resource Groups (изоляция команд)

```json
{
  "rootGroups": [
    {
      "name": "global",
      "softMemoryLimit": "80%",
      "hardConcurrencyLimit": 100,
      "subGroups": [
        {"name": "data-platform", "softMemoryLimit": "50%", "hardConcurrencyLimit": 40},
        {"name": "analytics",     "softMemoryLimit": "30%", "hardConcurrencyLimit": 30},
        {"name": "ml",            "softMemoryLimit": "20%", "hardConcurrencyLimit": 30}
      ]
    }
  ]
}
```
