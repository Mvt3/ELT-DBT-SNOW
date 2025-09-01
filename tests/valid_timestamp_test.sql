SELECT
    rating_timestamp
FROM {{ ref('fct_ratings') }}
WHERE rating_timestamp < TO_TIMESTAMP_NTZ('1970-01-01 00:00:00')
   OR rating_timestamp > CURRENT_TIMESTAMP
