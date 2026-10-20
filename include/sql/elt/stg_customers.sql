-- include/sql/elt/stg_customers.sql
-- Raw CRM export -> typed, deduplicated, PII-masked, quality-flagged snapshot.
-- Raw name/email/phone never leave this file: only masked columns are stored.

CREATE OR REPLACE TABLE stg_customers AS
WITH typed AS (
    SELECT r._source_file, r._row_num, r._loaded_at,
           r.loyalty_tier                                          AS tier_raw,
           nullif(trim(r.customer_id), '')                         AS customer_id,
           nullif(trim(r.full_name), '')                           AS full_name,
           nullif(trim(r.email), '')                               AS email,
           nullif(trim(r.phone), '')                               AS phone,
           nullif(trim(r.city), '')                                AS city,
           nullif(trim(r.area), '')                                AS area,
           nullif(trim(r.pincode), '')                             AS pincode,
           CAST(try_strptime(trim(r.signup_date), '%Y-%m-%d') AS DATE) AS signup_date,
           CASE WHEN nullif(trim(r.loyalty_tier), '') IS NOT NULL
                THEN upper(left(trim(r.loyalty_tier), 1)) || lower(substr(trim(r.loyalty_tier), 2)) END AS tier_std,
           CASE upper(trim(r.is_active)) WHEN 'Y' THEN true WHEN 'N' THEN false END AS is_active
    FROM cur_raw_customers r
),
ranked AS (
    SELECT *,
           row_number() OVER (PARTITION BY customer_id ORDER BY _row_num) AS key_rank,
           row_number() OVER (PARTITION BY lower(email) ORDER BY _row_num) AS email_rank,
           coalesce(tier_std IN ('Bronze', 'Silver', 'Gold', 'Platinum'), false) AS tier_valid,
           coalesce(regexp_full_match(email, '^[^@\s]+@[^@\s]+\.[^@\s]+$'), false) AS email_valid,
           coalesce(regexp_full_match(phone, '\d{10}'), false) AS phone_valid,
           string_split(regexp_replace(full_name, '\s+', ' ', 'g'), ' ') AS name_parts
    FROM typed
),
checked AS (
    SELECT ranked.*,
        list_filter([
            CASE WHEN customer_id IS NULL THEN 'customers.customer_id_not_null' END,
            CASE WHEN customer_id IS NOT NULL AND key_rank > 1 THEN 'customers.customer_id_unique' END,
            CASE WHEN signup_date IS NULL THEN 'customers.signup_date_valid' END,
            CASE WHEN NOT tier_valid THEN 'customers.loyalty_tier_valid' END,
            CASE WHEN CAST(signup_date AS TIMESTAMP) > (SELECT run_ts FROM run_ctx)
                 THEN 'customers.signup_date_not_future' END
        ], x -> x IS NOT NULL) AS crit_failed,
        list_filter([
            CASE WHEN city IS NULL THEN 'customers.city_not_null' END,
            CASE WHEN email IS NOT NULL AND email_rank > 1 THEN 'customers.email_unique' END,
            CASE WHEN NOT email_valid THEN 'customers.email_format' END,
            CASE WHEN NOT phone_valid THEN 'customers.phone_10_digits' END,
            CASE WHEN NOT coalesce(regexp_full_match(pincode, '\d{6}'), false) THEN 'customers.pincode_6_digits' END,
            CASE WHEN tier_valid AND tier_raw <> tier_std THEN 'customers.loyalty_tier_casing' END
        ], x -> x IS NOT NULL) AS warn_failed
    FROM ranked
)
SELECT customer_id,
       CASE WHEN full_name IS NULL THEN NULL
            WHEN len(name_parts) = 1 THEN name_parts[1]
            ELSE name_parts[1] || ' ' || left(name_parts[-1], 1) || '.' END              AS customer_name_masked,
       CASE WHEN email_valid
            THEN left(split_part(email, '@', 1), 2) || '***@' || split_part(email, '@', 2) END AS email_masked,
       CASE WHEN phone_valid THEN left(phone, 2) || '******' || right(phone, 2) END         AS phone_masked,
       city,
       area,
       pincode,
       signup_date,
       tier_std                                AS loyalty_tier,
       is_active,
       CASE WHEN len(crit_failed) > 0 THEN 'FAILED'
            WHEN len(warn_failed) > 0 THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       nullif(array_to_string(crit_failed, '; '), '') AS failure_reason,
       nullif(array_to_string(warn_failed, '; '), '') AS dq_warnings,
       crit_failed,
       warn_failed,
       _source_file,
       _row_num,
       _loaded_at
FROM checked
ORDER BY _row_num;

DELETE FROM stg_dq_failures
WHERE run_id = (SELECT run_id FROM run_ctx) AND source_table = 'customers';

INSERT INTO stg_dq_failures
SELECT ctx.run_id, 'customers', s._source_file, r.customer_id, s._row_num, s.failure_reason,
       CAST(to_json(r) AS VARCHAR), ctx.checked_at
FROM stg_customers s
JOIN cur_raw_customers r ON r._source_file = s._source_file AND r._row_num = s._row_num
CROSS JOIN run_ctx ctx
WHERE s.dq_status = 'FAILED';
