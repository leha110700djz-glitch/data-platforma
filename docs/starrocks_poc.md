# StarRocks 3.2 — POC Summary

## Контекст

POC: сравнить StarRocks с ClickHouse на TPC-DS workload (ритейл-сценарий:
агрегации по заказам, джойны с размерностями, range scans по дате).

## Setup
- 3 узла StarRocks 3.2 (1 FE + 2 BE), each 16 vCPU / 64 GB RAM
- External catalog на Iceberg (S3), без копирования данных
- 2 Materialized Views: daily_revenue_by_store, monthly_revenue_by_city

## Результаты

| Запрос | ClickHouse 23 | StarRocks 3.2 | Комментарий |
|--------|---------------|---------------|-------------|
| Q1: SUM(revenue) GROUP BY store (1 day) | 0.3 s | 0.4 s | Comparable |
| Q2: 6-way join orders × dim × customers | 12 s | 4 s | StarRocks 3× быстрее за счёт MV + adaptive join |
| Q3: Range scan 1 year | 8 s | 6 s | Comparable |
| Q4: INSERT INTO mart SELECT ... | 95 s | 70 s | StarRocks быстрее через pipeline engine |

## Вывод

- StarRocks сильнее на multi-join OLAP-нагрузке → кандидат для BI-витрин
- ClickHouse остаётся для time-series и простых агрегаций (logs, events)
- Дальнейший шаг: совместить StarRocks + ClickHouse под разными workload'ами

Уровень: **Base** (POC, не production) — план Q1 2027 пилот на одном домене.
