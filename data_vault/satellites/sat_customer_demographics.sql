-- Data Vault 2.0 Satellite: sat_customer_demographics (SD2 pattern)
-- Tracks changes to customer attributes over time (SCD via satellites)
-- SD2: Satellite of Satellite — separates frequently-changing attributes from slowly-changing

CREATE TABLE IF NOT EXISTS analytics.sat_customer_demographics (
    customer_hk              STRING      NOT NULL,
    load_datetime            TIMESTAMP   NOT NULL,
    record_source            STRING      NOT NULL,
    email                    STRING,
    phone                    STRING,
    city                     STRING,
    hash_diff                STRING      NOT NULL,           -- MD5 of concatenated non-key attributes → detect change
    effective_from           TIMESTAMP   NOT NULL,            -- business-effective date
    effective_to             TIMESTAMP,                       -- NULL = current record
    PRIMARY KEY (customer_hk, load_datetime)
)
USING ICEBERG
PARTITIONED BY (days(load_datetime));

-- Insert only changed records (compare hash_diff against latest satellite record)
INSERT INTO analytics.sat_customer_demographics
SELECT
    h.customer_hk,
    current_timestamp()       AS load_datetime,
    '_raw_customers'          AS record_source,
    src.email,
    src.phone,
    src.city,
    md5(concat_ws('|', coalesce(src.email, ''), coalesce(src.phone, ''), coalesce(src.city, ''))) AS hash_diff,
    src.updated_ts            AS effective_from,
    CAST(NULL AS TIMESTAMP)   AS effective_to
FROM   {{ source('raw', 'customers') }} src
JOIN   analytics.hub_customer h ON h.customer_id = src.customer_id
LEFT JOIN (
    SELECT customer_hk, hash_diff
    FROM (SELECT customer_hk, hash_diff, row_number() OVER (PARTITION BY customer_hk ORDER BY load_datetime DESC) AS rn
          FROM analytics.sat_customer_demographics) t WHERE rn = 1
) last_sat ON last_sat.customer_hk = h.customer_hk
WHERE last_sat.hash_diff IS NULL
   OR last_sat.hash_diff <> md5(concat_ws('|', coalesce(src.email, ''), coalesce(src.phone, ''), coalesce(src.city, '')));

-- Close-out previous record (set effective_to) — part of SD2 / SCD2 implementation
MERGE INTO analytics.sat_customer_demographics AS tgt
USING (
    SELECT customer_hk, max(effective_from) AS effective_from
    FROM analytics.sat_customer_demographics
    GROUP BY customer_hk
) src
ON  tgt.customer_hk = src.customer_hk
AND tgt.effective_to IS NULL
AND tgt.effective_from < src.effective_from
WHEN MATCHED THEN UPDATE SET effective_to = src.effective_from;
