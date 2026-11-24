-- Data Vault 2.0 Hub: hub_customer
-- Business key: customer_id (from source PostgreSQL)
-- Hash key: SHA-256(customer_id || '|customers')
-- Pattern: idempotent MERGE — load only new business keys

CREATE TABLE IF NOT EXISTS analytics.hub_customer (
    customer_hk        STRING       NOT NULL,
    customer_id        BIGINT       NOT NULL,
    load_datetime      TIMESTAMP    NOT NULL,
    record_source      STRING       NOT NULL
)
USING ICEBERG
PARTITIONED BY (days(load_datetime))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

-- Idempotent load: only insert business keys not yet in hub
INSERT INTO analytics.hub_customer
SELECT
    sha2(concat_ws('|', coalesce(cast(customer_id as string), ''), 'customers'), 256) AS customer_hk,
    customer_id,
    current_timestamp()      AS load_datetime,
    '_raw_customers'         AS record_source
FROM   {{ source('raw', 'customers') }} src
WHERE NOT EXISTS (
    SELECT 1 FROM analytics.hub_customer h WHERE h.customer_id = src.customer_id
);
