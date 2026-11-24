-- Data Vault 2.0 Link: link_order_customer
-- Relationship between order and customer (many-to-one in this case, but modelled as link for generality)
-- Hash key: SHA-256(order_id || '|' || customer_id || '|order_customer')

CREATE TABLE IF NOT EXISTS analytics.link_order_customer (
    link_order_customer_hk   STRING      NOT NULL,
    order_hk                 STRING      NOT NULL,
    customer_hk              STRING      NOT NULL,
    load_datetime            TIMESTAMP   NOT NULL,
    record_source            STRING      NOT NULL
)
USING ICEBERG
PARTITIONED BY (days(load_datetime));

INSERT INTO analytics.link_order_customer
SELECT
    sha2(concat_ws('|',
        o.order_id,
        o.customer_id,
        'order_customer'
    ), 256)                AS link_order_customer_hk,
    sha2(concat_ws('|', o.order_id, 'orders'), 256)         AS order_hk,
    sha2(concat_ws('|', o.customer_id, 'customers'), 256)   AS customer_hk,
    current_timestamp()    AS load_datetime,
    '_raw_orders'          AS record_source
FROM   {{ source('raw', 'orders') }} o
WHERE NOT EXISTS (
    SELECT 1 FROM analytics.link_order_customer l
    WHERE l.order_hk = sha2(concat_ws('|', o.order_id, 'orders'), 256)
);
