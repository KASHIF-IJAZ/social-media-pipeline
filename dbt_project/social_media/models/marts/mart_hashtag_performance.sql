/*
  mart_hashtag_performance.sql

  This model explodes the hashtags JSON array into individual rows.
  One post with 3 hashtags becomes 3 rows — one per hashtag.
  This is called "unnesting" or "exploding" arrays — common in real pipelines.
*/

WITH

posts_with_tags AS (
    SELECT
        post_id,
        post_day,
        post_week,
        post_month,
        like_count,
        comment_count,
        repost_count,
        view_count,
        engagement_score,
        is_viral,
        -- Unnest the JSON hashtag array into individual rows
        UNNEST(
            FROM_JSON(hashtags, '["varchar"]')
        )                                   AS hashtag
    FROM {{ ref('stg_posts') }}
    WHERE hashtag_count > 0
)

SELECT
    hashtag,
    COUNT(DISTINCT post_id)                AS total_posts,
    SUM(like_count)                        AS total_likes,
    SUM(comment_count)                     AS total_comments,
    SUM(repost_count)                      AS total_reposts,
    SUM(view_count)                        AS total_views,
    ROUND(AVG(engagement_score), 4)        AS avg_engagement,
    COUNT(CASE WHEN is_viral THEN 1 END)   AS viral_posts,
    -- trending score = engagement weighted by recency
    ROUND(
        SUM(like_count + comment_count * 2 + repost_count * 3)
        / NULLIF(COUNT(DISTINCT post_id), 0),
    2)                                     AS trending_score,
    MIN(post_day)                          AS first_seen,
    MAX(post_day)                          AS last_seen,
    CURRENT_TIMESTAMP                      AS built_at

FROM posts_with_tags
GROUP BY hashtag
ORDER BY trending_score DESC