# Observability — Grafana 10 / Prometheus 2.50

## Метрики (Prometheus)

### Airflow exporter
- `airflow_task_duration_seconds` — per-task latency p50/p95/p99
- `airflow_dag_status` (labels: dag_id, status)
- `airflow_scheduler_heartbeat_age_seconds`
- `airflow_pool_used_slots` (label: pool)

### Spark pushgateway
- `spark_app_duration_seconds`
- `spark_stage_input_bytes`
- `spark_stage_shuffle_bytes_written`
- `spark_executor_memory_used_bytes`
- `spark_skew_partition_ratio` (custom metric: maxPartitionSize / medianPartitionSize)

### Data Quality
- `dq_suite_passed_total` (label: suite_name)
- `dq_suite_failed_total`
- `dq_row_count` (label: table)

## Dashboards (Grafana)

1. **Pipeline Health** — DAG status, SLA breaches, retry counts
2. **Spark Performance** — stage durations, shuffle bytes, skew ratio
3. **Data Quality** — suite pass rate, row counts, drift
4. **Storage** — S3 size per layer, Iceberg snapshot count, Parquet file count

## Alerting rules

```yaml
groups:
  - name: data-platform
    rules:
      - alert: AirflowDagFailed
        expr: airflow_dag_status{status="failed"} > 0
        for: 5m
        annotations:
          summary: "DAG {{ $labels.dag_id }} failed"
          runbook: "https://wiki.data-platform.internal/runbooks/airflow-dag-failed"

      - alert: PipelineSLABreach
        expr: airflow_task_duration_seconds{task_id=~".*extract.*"} > 3600
        for: 2m
        annotations:
          summary: "Task {{ $labels.task_id }} exceeded SLA (1h)"

      - alert: DataQualityFailed
        expr: dq_suite_failed_total > 0
        for: 1m
        annotations:
          summary: "DQ suite {{ $labels.suite_name }} failed"

      - alert: SparkSkewDetected
        expr: spark_skew_partition_ratio > 5
        for: 5m
        annotations:
          summary: "Spark stage skew detected — ratio {{ $value }}"
```
