/*
  mart_daily_engagement.sql — Gold layer

  Business question: How is engagement trending day by day?

  This model joins staging tables — note how we use
  {{ ref('stg_posts') }} instead of source().
  ref() = reference another dbt model (creates lineage)
  source() = reference raw bronze data
*/

SELECT
    p.post_day                              AS date,
    COUNT(DISTINCT p.post_id)              AS total_posts,
    COUNT(DISTINCT p.user_id)              AS active_authors,
    SUM(p.like_count)                      AS total_likes,
    SUM(p.comment_count)                   AS total_comments,
    SUM(p.repost_count)                    AS total_reposts,
    SUM(p.view_count)                      AS total_views,
    ROUND(AVG(p.engagement_score), 4)      AS avg_engagement_score,
    COUNT(CASE WHEN p.is_viral
               THEN 1 END)                 AS viral_posts,
    COUNT(CASE WHEN p.engagement_tier
               = 'high' THEN 1 END)        AS high_engagement_posts,
    -- engagement rate = total interactions / total views
    ROUND(
        CAST(
            SUM(p.like_count) +
            SUM(p.comment_count) +
            SUM(p.repost_count)
        AS DOUBLE)
        / NULLIF(SUM(p.view_count), 0) * 100,
    4)                                     AS platform_engagement_rate,
    CURRENT_TIMESTAMP                      AS built_at

FROM {{ ref('stg_posts') }} p
GROUP BY p.post_day
ORDER BY p.post_day DESC