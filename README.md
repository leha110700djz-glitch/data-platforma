# data-platform

> Data-platform монорепозиторий «под ключ» — демонстрация стека Senior Data Engineer: Airflow + dbt + Spark + Data Vault 2.0 + Iceberg + DQ + Cube + k8s + CI/CD.

**Автор:** Червак Алексей Петрович · [@dobrycode](https://github.com/dobrycode) · chervak_2016@mail.ru

---

## Что внутри

Репозиторий содержит рабочий код по каждому из ключевых навыков Senior Data Engineer. Это не учебные сниппеты, а выдержки из паттернов, применяемых в Сбере / МТС / ИП-проекте, переработанные под ритейл/финанс/телеком-домен (товары, заказы, клиенты).

| # | Навык | Где смотреть | Уровень |
|---|-------|--------------|---------|
| 1 | **Python** (ООП, async, типизация, тесты, профилирование, API-клиенты) | [`spark/jobs/`](spark/jobs/) · [`airflow/plugins/hooks/`](airflow/plugins/hooks/) | Pro · 3.11/3.12 |
| 2 | **Spark** (pipelines, оптимизация join/skew, Structured Streaming) | [`spark/jobs/etl_sales_daily.py`](spark/jobs/etl_sales_daily.py) · [`spark/jobs/write_iceberg.py`](spark/jobs/write_iceberg.py) | Pro · 3.5 |
| 3 | **SQL** (EXPLAIN, индексы, узкие места, планы выполнения) | [`dbt/models/`](dbt/models/) · [`data_vault/`](data_vault/) | Pro · ANSI SQL |
| 4 | **Airflow** (модульные/динамические DAG, кастомные операторы/сенсоры/хуки, XCom/пулы/SLA) | [`airflow/dags/sales_daily_dag.py`](airflow/dags/sales_daily_dag.py) · [`airflow/dags/dynamic_sources_dag.py`](airflow/dags/dynamic_sources_dag.py) · [`airflow/plugins/hooks/sqla_hook.py`](airflow/plugins/hooks/sqla_hook.py) | Pro · 2.9/2.10 |
| 5 | **dbt** (staging/intermediate/marts, incremental, snapshots, hooks, кастомные макросы/тесты, Slim CI) | [`dbt/`](dbt/) — models/macros/tests/snapshots/dbt_project.yml | Pro · 1.7/1.8 |
| 6 | **Git** (rebase, cherry-pick, конфликты, теги релизов) | `git log` история (после публикации) — теги `vX.Y.Z` | Pro · 2.40+ |
| 7 | **CI/CD** (`.gitlab-ci.yml`, stages/jobs/rules/artifacts/cache/secrets, dev/stage/prod, MR-pipelines, approvals) | [`.gitlab-ci.yml`](.gitlab-ci.yml) | Advanced · GitLab 16.x |
| 8 | **Kubernetes** (Helm, multi-stage Docker, KubernetesPodOperator) | [`k8s/helm/values.yaml`](k8s/helm/values.yaml) · [`docker/Dockerfile.airflow`](docker/Dockerfile.airflow) · [`airflow/dags/spark_k8s_dag.py`](airflow/dags/spark_k8s_dag.py) | Advanced · k8s 1.29/1.30 |
| 9 | **Архитектуры хранения** (Parquet/Iceberg, слои, обоснование) | [`docs/storage_decision.md`](docs/storage_decision.md) · [`spark/jobs/write_iceberg.py`](spark/jobs/write_iceberg.py) | Pro · Parquet 1.13 / Iceberg 1.4 |
| 10 | **S3** (lifecycle, versioning, automation) | [`scripts/s3_lifecycle.py`](scripts/s3_lifecycle.py) | Advanced · AWS S3 / MinIO |
| 11 | **Data Vault 2.0** (Hub/Link/Satellite, hash-ключи, SD2) | [`data_vault/`](data_vault/) · [`dbt/macros/hash_key.sql`](dbt/macros/hash_key.sql) | Advanced · DV 2.0 |
| 12 | **Semantic Layer** (Cube-core, MetricFlow, метрики, метаданные) | [`semantic_layer/cube/schemas/order_revenue.yml`](semantic_layer/cube/schemas/order_revenue.yml) | Advanced · Cube 0.34+ |
| 13 | **СУБД** (схемы под нагрузку, партиционирование, шардирование, репликация/бэкапы/миграции, блокировки/изоляция) | [`dbt/models/marts/fact_sales_daily.sql`](dbt/models/marts/fact_sales_daily.sql) · [`docs/db_design.md`](docs/db_design.md) | Pro · PG 14-16 / ClickHouse 23/24 |
| 14 | **Мониторинг** (дашборды, алерты, метрики) | [`docs/observability.md`](docs/observability.md) · [`.gitlab-ci.yml`](.gitlab-ci.yml) alert job | Pro · Grafana 10/11 · Prometheus 2.50+ |
| 15 | **Data Quality** (Great Expectations, метрики качества) | [`dq/great_expectations/`](dq/great_expectations/) | Pro · GE 0.18/1.x |
| 16 | **DataHub** (каталог, lineage в CI/CD) | [`datahub/recipes/airflow_to_datahub.yml`](datahub/recipes/airflow_to_datahub.yml) · [`.gitlab-ci.yml`](.gitlab-ci.yml) ingest job | Advanced · 0.13+ |
| 17 | **Дата-контракты** (составление и изменение) | [`dbt/contracts/orders_contract.yml`](dbt/contracts/orders_contract.yml) · [`dbt/contracts/customer_contract.yml`](dbt/contracts/customer_contract.yml) | Advanced · dbt 1.7 contracts |
| 18 | **Trino / StarRocks** (federation-запросы) | [`docs/trino_catalog.md`](docs/trino_catalog.md) · [`docs/starrocks_poc.md`](docs/starrocks_poc.md) | Trino Advanced · StarRocks Base |

---

## Архитектура платформы

```
                ┌─────────────┐    ┌──────────────┐    ┌────────────┐
 Источники      │  PostgreSQL │    │ Kafka 3.6    │    │ S3 (raw)   │
                │  (orders,   │    │ (clickstream,│    │ (parquet)  │
                │  customers) │    │  events)     │    │            │
                └──────┬──────┘    └──────┬───────┘    └─────┬──────┘
                       │                  │                  │
                       ▼                  ▼                  ▼
                ┌──────────────────────────────────────────────────┐
Bronze (raw)    │  Apache Iceberg 1.4 на S3  (time-travel, schema   │
                │  evolution)  + Parquet 1.13 (zstd, row-group 128M)│
                └──────────────────────┬───────────────────────────┘
                                       │
                       ┌───────────────┴────────────────┐
                       ▼                                ▼
                ┌─────────────┐                ┌─────────────────┐
Silver          │ dbt 1.7     │                │ Spark 3.5       │
(staging/inter) │  staging    │                │ PySpark jobs    │
                │  models     │                │ (DQ, optimize)  │
                └──────┬──────┘                └────────┬────────┘
                       │                                │
                       └───────────────┬────────────────┘
                                       ▼
                ┌──────────────────────────────────────────────────┐
Gold (marts)    │ dbt marts (fact_*, dim_*, mart_*)  +  Data Vault  │
                │ 2.0 (hubs, links, satellites)  +  ClickHouse 23   │
                │ (OLAP marts под BI)                              │
                └──────────────┬───────────────────────────────────┘
                               │
                ┌──────────────┴───────────────┐
                ▼                              ▼
        ┌──────────────┐              ┌────────────────┐
        │ Cube-core    │              │ DataHub 0.13   │
        │ 0.34 (REST)  │              │ (catalog +     │
        │  semantic    │              │  lineage)      │
        │  layer       │              │                │
        └──────────────┘              └────────────────┘

Orchestration:  Apache Airflow 2.9  (TaskFlow API + KubernetesPodOperator)
DQ:             Great Expectations 0.18  (suite + checkpoint в DAG)
CI/CD:          GitLab CI 16.x  (lint → test → dbt docs → build → deploy)
Runtime:        Kubernetes 1.29  (Helm chart + multi-stage Docker <350 МБ)
Observability:  Grafana 10 + Prometheus 2.50
```

---

## Структура репозитория

```
data-platform/
├── README.md                              ← этот файл
├── PUBLISH_GUIDE.md                       ← пошаговая инструкция публикации на GitHub
├── .gitlab-ci.yml                         ← CI/CD: lint/test/dbt docs/build/deploy
├── .gitignore
├── airflow/
│   ├── dags/
│   │   ├── sales_daily_dag.py             ← TaskFlow API, SLA, XCom, пулы
│   │   ├── dynamic_sources_dag.py         ← динамические DAG по конфигу sources.yml
│   │   └── spark_k8s_dag.py               ← KubernetesPodOperator
│   └── plugins/
│       ├── hooks/sqla_hook.py             ← кастомный SQLAlchemyHook (async)
│       └── operators/dq_checkpoint_operator.py  ← Great Expectations operator
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/stg_orders.sql         ← incremental (insert_overwrite)
│   │   ├── staging/stg_customers.sql
│   │   ├── intermediate/int_orders_enriched.sql
│   │   ├── marts/fact_sales_daily.sql     ← партиционирование по дате
│   │   └── marts/mart_customer_rfm.sql
│   ├── snapshots/snap_customer_demographics.sql  ← SCD2
│   ├── macros/hash_key.sql                ← DV 2.0 hash-ключ SHA-256
│   ├── tests/not_null_hash_key.sql        ← кастомный тест
│   └── contracts/
│       ├── orders_contract.yml            ← дата-контракт (schema + SLA + SLO + owner)
│       └── customer_contract.yml
├── spark/jobs/
│   ├── etl_sales_daily.py                 ← PySpark 3.5 + оптимизация skew
│   ├── write_iceberg.py                   ← Iceberg 1.4 MERGE INTO + time travel
│   └── parquet_optimize.py                ← zstd + row-group 128 МБ
├── data_vault/
│   ├── hubs/hub_customer.sql
│   ├── links/link_order_customer.sql
│   └── satellites/sat_customer_demographics.sql   ← SD2 (load_date, record_source)
├── dq/great_expectations/
│   ├── expectations/customer_suite.json
│   └── checkpoints/customer_checkpoint.yml
├── semantic_layer/cube/schemas/
│   └── order_revenue.yml                  ← measures, dimensions, pre-aggregations
├── k8s/helm/values.yaml                   ← Airflow on k8s (official Helm chart)
├── docker/Dockerfile.airflow              ← multi-stage, итоговый образ <350 МБ
├── datahub/recipes/airflow_to_datahub.yml ← ingestion в CI/CD
├── scripts/s3_lifecycle.py                ← lifecycle rules + versioning (boto3)
└── docs/
    ├── storage_decision.md                ← Parquet vs Iceberg vs Delta
    ├── db_design.md                       ← PG/ClickHouse схемa под нагрузку
    ├── observability.md                   ← Grafana/Prometheus дашборды и алерты
    ├── trino_catalog.md                   ← federation через Trino 428
    └── starrocks_poc.md                   ← POC StarRocks 3.2 на Iceberg
```

---

## Измеримые результаты (кратко)

| Проект | Результат |
|--------|-----------|
| Сбер | SLA 99.9% на 10+ ТБ/сут, ускорение витрин 60% (2ч→45мин), инциденты с битыми данными -80% |
| МТС | -40% времени пайплайнов (3ч→1.5ч), доля некорректных данных <0.5% |
| ИП Червак | 34 DQ-теста, 0 инцидентов за 5 мес, Qini +0.25 на uplift |
| EY | ускорение отчётов 15мин→2мин, -70% ручной обработки |

Полная матрица навыков с уровнями (Base/Advanced/Pro) и версиями — в файле `skills_matrix.xlsx` (отправляется отдельно).

---

## Лицензия

MIT — код можно свободно переиспользовать. Контактные данные и упоминания работодателей — приватные.

---

## Контакты

- **Telegram:** [@dobrycode](https://t.me/dobrycode)
- **Email:** chervak_2016@mail.ru
- **Phone:** +7 (993) 622-81-82


---

*Репозиторий подготовлен как portfolio для отклика на позиции Senior Data Engineer / Data Platform Engineer.*
