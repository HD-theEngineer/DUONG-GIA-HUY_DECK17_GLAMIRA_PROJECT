WITH dim_user__load AS (
    SELECT
        *
    FROM
        {{ ref("stg_glamira__user")}}
)

SELECT
    *
FROM
    dim_user__load
ORDER BY user_key