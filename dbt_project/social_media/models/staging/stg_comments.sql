WITH

raw AS (
    SELECT * FROM {{ source('bronze', 'comments') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY comment_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM raw
)

SELECT
    comment_id,
    post_id,
    user_id,
    TRIM(content)                           AS content,
    CAST(like_count AS INTEGER)             AS like_count,
    LOWER(sentiment)                        AS sentiment,
    CAST(is_deleted AS BOOLEAN)             AS is_deleted,
    CAST(commented_at AS TIMESTAMP)         AS commented_at,
    DATE_TRUNC('day',
        CAST(commented_at AS TIMESTAMP))    AS comment_day,
    CAST(ingested_at AS TIMESTAMP)          AS ingested_at,
    source_system,
    CURRENT_TIMESTAMP                       AS transformed_at

FROM deduped
WHERE rn = 1
  AND comment_id  IS NOT NULL
  AND post_id     IS NOT NULL
  AND is_deleted  = false