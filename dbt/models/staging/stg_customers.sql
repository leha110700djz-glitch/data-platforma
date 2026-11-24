{{
  config(
    materialized='view',
    tags=['staging', 'hourly'],
    description='Customers from source PostgreSQL with normalized fields'
  )
}}

select
    customer_id::bigint        as customer_id,
    nullif(email, '')::varchar as email,
    nullif(phone, '')::varchar as phone,
    registration_ts::timestamp as registration_ts,
    city::varchar              as city,
    '_raw_customers'::varchar  as record_source,
    {{ load_datetime() }}      as load_datetime
from {{ source('raw', 'customers') }}
