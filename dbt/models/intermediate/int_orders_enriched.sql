{{
  config(
    materialized='ephemeral',
    tags=['intermediate'],
    description='Orders joined with customers + product info, deduped'
  )
}}

with
orders as (select * from {{ ref('stg_orders') }}),
customers as (select * from {{ ref('stg_customers') }}),

enriched as (
    select
        o.order_id,
        o.customer_id,
        o.product_id,
        o.quantity,
        o.unit_price,
        o.quantity * o.unit_price            as revenue,
        o.order_ts,
        o.order_date,
        o.status,
        o.store_id,
        c.city,
        c.registration_ts,
        {{ hash_key(['o.customer_id']) }}     as customer_hk,    -- DV 2.0 hash key
        {{ hash_key(['o.order_id']) }}        as order_hk
    from orders o
    left join customers c using (customer_id)
)

select * from enriched
