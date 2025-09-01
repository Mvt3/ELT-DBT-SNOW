WITH src_score AS (
    select * from {{ ref('src_genome_scores') }}
)

SELECT movie_id, tag_id, ROUND(relevance,4) AS relevance_score
FROM src_score
WHERE relevance > 0 