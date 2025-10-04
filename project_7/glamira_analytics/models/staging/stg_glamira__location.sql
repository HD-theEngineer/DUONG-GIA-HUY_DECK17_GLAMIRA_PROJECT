WITH stg_glamira__location_rename AS (
    SELECT
        country_short           AS country_short_name,
        country_long            AS country_name,
        region                  AS region_name,
        city                    AS city_name,
        ip                      AS ip_address
    FROM 
        {{ ref("stg_glamira__raw_ip2location")}}
)

, stg_glamira__location_handle_invalid AS (
    SELECT
        CASE 
            WHEN TRIM(country_short_name) = '-' 
                OR TRIM(country_short_name) = ''
                OR country_short_name IS NULL
                THEN 'XNA' 
            ELSE UPPER(TRIM(country_short_name))
        END                                     AS country_short_name,
        CASE 
            WHEN TRIM(country_name) = '-' 
                OR TRIM(country_name) = ''
                OR country_name IS NULL
                THEN 'XNA' 
            ELSE UPPER(TRIM(country_name))
        END                                     AS country_name,
        CASE 
            WHEN TRIM(region_name) = '-' 
                OR TRIM(region_name) = ''
                OR region_name IS NULL
                THEN 'XNA' 
            ELSE UPPER(TRIM(region_name))
        END                                     AS region_name, 
        CASE 
            WHEN TRIM(city_name) = '-' 
                OR TRIM(city_name) = ''
                OR city_name IS NULL
                THEN 'XNA' 
            ELSE UPPER(TRIM(city_name))
        END                                     AS city_name,        
        ip_address
    FROM 
        stg_glamira__location_rename
)

, stg_glamira__location_gen_key AS (
    SELECT 
        FARM_FINGERPRINT(country_name || region_name || city_name) AS location_key,
        *
    FROM 
        stg_glamira__location_handle_invalid
)

SELECT * FROM stg_glamira__location_gen_key