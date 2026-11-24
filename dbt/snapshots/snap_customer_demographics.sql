{% snapshot snap_customer_demographics %}

{{
  config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_ts',
    hard_deletes='invalidate',
    tags=['snapshots', 'scd2']
  )
}}

select
    customer_id,
    email,
    phone,
    city,
    registration_ts,
    updated_ts
from {{ source('raw', 'customers') }}

{% endsnapshot %}
