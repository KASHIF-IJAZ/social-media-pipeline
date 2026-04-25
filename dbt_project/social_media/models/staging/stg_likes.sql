WITH

raw AS (
    SELECT * FROM {{ source('bronze', 'likes') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY like_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM raw
)

SELECT
    like_id,
    post_id,
    user_id,
    LOWER(reaction_type)                    AS reaction_type,
    CAST(liked_at AS TIMESTAMP)             AS liked_at,
    DATE_TRUNC('day',
        CAST(liked_at AS TIMESTAMP))        AS like_day,
    CAST(ingested_at AS TIMESTAMP)          AS ingested_at,
    source_system,
    CURRENT_TIMESTAMP                       AS transformed_at

FROM deduped
WHERE rn = 1
  AND like_id IS NOT NULL
  AND post_id IS NOT NULL