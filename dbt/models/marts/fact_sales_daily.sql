{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['order_date', 'store_id', 'product_id'],
    partition_by={'field': 'order_date', 'data_type': 'date'},
    file_format='iceberg',
    iceberg_expire_snapshots_hours=24 * 7,
    on_schema_change='fail',
    contracts={"enforced": true},
    post_hook=[
      'ANALYZE {{ this }}',
      '{{ refresh_cube_preagg("order_revenue") }}'
    ],
    description='Daily sales fact table — incremental MERGE on (order_date, store_id, product_id)'
  )
}}

with src as (select * from {{ ref('int_orders_enriched') }}),

agg as (
    select
        order_date,
        store_id,
        product_id,
        count(distinct order_id)     as orders_count,
        sum(quantity)                as quantity_total,
        sum(revenue)                 as revenue_total,
        avg(revenue)                 as avg_order_value,
        min(order_ts)                as first_order_ts,
        max(order_ts)                as last_order_ts,
        {{ load_datetime() }}        as load_datetime
    from src
    {% if is_incremental() %}
      where order_date >= (select coalesce(max(order_date), date '1970-01-01') from {{ this }})
    {% endif %}
    group by 1, 2, 3
)

select * from agg
