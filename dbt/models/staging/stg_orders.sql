{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'order_date', 'data_type': 'date'},
    file_format='parquet',
    unique_key='order_id',
    contracts={"enforced": true},
    tags=['staging', 'hourly'],
    description='Cleaned orders from source PostgreSQL — incremental by order_date'
  )
}}

with source as (
    select
        order_id::bigint          as order_id,
        customer_id::bigint       as customer_id,
        product_id::bigint        as product_id,
        quantity::int             as quantity,
        unit_price::numeric(12,2) as unit_price,
        order_ts::timestamp       as order_ts,
        date(order_ts)            as order_date,
        lower(status)::varchar    as status,
        store_id::int             as store_id,
        '_raw_orders'::varchar    as record_source,
        {{ load_datetime() }}     as load_datetime
    from {{ source('raw', 'orders') }}
    {% if is_incremental() %}
      where order_date >= (select coalesce(max(order_date), date '1970-01-01') from {{ this }})
    {% endif %}
)

select * from source

{{ contract_check('orders_contract') }}
