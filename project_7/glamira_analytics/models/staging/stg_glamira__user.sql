WITH stg_glamira__user_rename AS (
    SELECT
        user_id_db                          AS user_key,
        email_address                       AS user_email_address
    FROM
        {{ ref("stg_glamira__temp_raw_data")}}
)

, stg_glamira__user_handle_invalid AS (
    SELECT
        CAST (
        CASE
            WHEN TRIM(user_key) = ''
                OR TRIM(user_key) = '-'
                THEN '-1'
            ELSE TRIM(user_key)
        END AS INTEGER)                     AS user_key,
        CASE 
            WHEN TRIM(user_email_address) = '' 
                OR TRIM(user_email_address) = '-'
                THEN 'XNA'
            ELSE TRIM(user_email_address)
        END                                 AS user_email_address
    FROM
        stg_glamira__user_rename
)

, stg_glamira__user_distinct AS (
    SELECT DISTINCT
        *
    FROM
        stg_glamira__user_handle_invalid
)

, stg_glamira__email_list_prepare AS (
    SELECT
        COALESCE(user_key,-1) AS user_key,
        COALESCE(user_email_address, 'XNA') AS user_email_address
    FROM
        stg_glamira__user_distinct
    ORDER BY
        user_key ASC, user_email_address ASC
)

, stg_glamira__aggregate_email AS (
    SELECT 
        user_key,
        ARRAY_AGG(user_email_address) AS email_list
    FROM 
        stg_glamira__email_list_prepare
    GROUP BY
        user_key
)

SELECT
    user_key,
    CASE 
        WHEN ARRAY_LENGTH(email_list) = 1
            THEN email_list[SAFE_OFFSET(0)]
        WHEN ARRAY_LENGTH(email_list) > 1
            AND email_list[SAFE_OFFSET(0)] = 'XNA'
            THEN email_list[SAFE_OFFSET(1)]
        ELSE email_list[SAFE_OFFSET(0)]
    END
    AS user_unique_email_address
FROM
    stg_glamira__aggregate_email