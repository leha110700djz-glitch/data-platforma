{{
  config(
    materialized='table',
    tags=['marts', 'daily', 'rfm'],
    description='Customer RFM segmentation — daily rebuild'
  )
}}

with
orders as (select * from {{ ref('int_orders_enriched') }} where status = 'completed'),

rfm as (
    select
        customer_id,
        date_diff('day', max(order_date), current_date)                as recency,
        count(distinct order_id)                                       as frequency,
        sum(revenue)                                                   as monetary,
        min(order_date)                                                as first_order,
        max(order_date)                                                as last_order
    from orders
    group by customer_id
),

segments as (
    select
        r.*,
        case
            when recency <= 30  and frequency >= 10 and monetary >= 50000 then 'Champions'
            when recency <= 90  and frequency >= 5                       then 'Loyal'
            when recency <= 30  and frequency < 5                        then 'New'
            when recency > 180                                          then 'At Risk'
            else 'Hibernating'
        end as rfm_segment
    from rfm r
)

select * from segments
