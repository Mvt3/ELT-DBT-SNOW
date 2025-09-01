WITH src_ratings AS (
    SELECT DISTINCT user_id FROM {{ ref('src_ratings') }}
),

src_tags AS (
    SELECT DISTINCT user_id FROM {{ ref('src_tags') }}
)


-- Union
select distinct user_id
from(
    select * from src_ratings
    union
    select * from src_tags
)