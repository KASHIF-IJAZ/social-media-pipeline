/*
  stg_posts.sql — Silver layer: clean posts

  Key transformations:
  1. Parse hashtags from JSON string → keep as string (explosion happens in marts)
  2. Normalise engagement score to 0-100 range
  3. Add time-based features (hour_of_day, day_of_week, is_weekend)
  4. Flag anomalies without dropping them
*/

WITH

raw AS (
    SELECT * FROM {{ source('bronze', 'posts') }}
),

-- Deduplication: same as project 1 pattern
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY post_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM raw
),

cleaned AS (
    SELECT
        -- identifiers
        post_id,
        user_id,

        -- content
        TRIM(content)                           AS content,
        LOWER(post_type)                        AS post_type,
        LOWER(language)                         AS language,
        media_url,

        -- hashtags (kept as JSON string — exploded in gold layer)
        hashtags,
        CAST(hashtag_count AS INTEGER)          AS hashtag_count,

        -- raw engagement counts
        CAST(like_count AS INTEGER)             AS like_count,
        CAST(comment_count AS INTEGER)          AS comment_count,
        CAST(repost_count AS INTEGER)           AS repost_count,
        CAST(view_count AS INTEGER)             AS view_count,

        -- normalised engagement score (0-100)
        -- formula: weighted interactions / views * 100, capped at 100
        LEAST(
            ROUND(
                (like_count * 1.0 +
                 comment_count * 2.0 +
                 repost_count * 3.0)
                / NULLIF(view_count, 0) * 100,
            4),
            100.0
        )                                       AS engagement_score,

        -- engagement tier
        CASE
            WHEN raw_engagement_score >= 5.0  THEN 'viral'
            WHEN raw_engagement_score >= 1.0  THEN 'high'
            WHEN raw_engagement_score >= 0.1  THEN 'medium'
            ELSE 'low'
        END                                     AS engagement_tier,

        -- flags
        CAST(is_viral AS BOOLEAN)               AS is_viral,
        CAST(is_deleted AS BOOLEAN)             AS is_deleted,
        CAST(is_nsfw AS BOOLEAN)                AS is_nsfw,

        -- location
        geo_country,

        -- time features (very useful for trend analysis)
        CAST(post_date AS TIMESTAMP)            AS post_date,
        DATE_TRUNC('hour',
            CAST(post_date AS TIMESTAMP))       AS post_hour,
        DATE_TRUNC('day',
            CAST(post_date AS TIMESTAMP))       AS post_day,
        DATE_TRUNC('week',
            CAST(post_date AS TIMESTAMP))       AS post_week,
        DATE_TRUNC('month',
            CAST(post_date AS TIMESTAMP))       AS post_month,
        EXTRACT(hour FROM
            CAST(post_date AS TIMESTAMP))       AS hour_of_day,
        EXTRACT(dow FROM
            CAST(post_date AS TIMESTAMP))       AS day_of_week,
        EXTRACT(dow FROM
            CAST(post_date AS TIMESTAMP))
            IN (0, 6)                           AS is_weekend,

        -- data quality flag
        CASE
            WHEN view_count < like_count
                THEN 'anomaly: likes exceed views'
            WHEN like_count < 0 OR view_count < 0
                THEN 'anomaly: negative counts'
            ELSE 'ok'
        END                                     AS dq_flag,

        -- pipeline metadata
        CAST(ingested_at AS TIMESTAMP)          AS ingested_at,
        source_system,
        CURRENT_TIMESTAMP                       AS transformed_at

    FROM deduped
    WHERE rn = 1
      AND post_id  IS NOT NULL
      AND user_id  IS NOT NULL
      AND post_date IS NOT NULL
      AND is_deleted = false
)

SELECT * FROM cleaned