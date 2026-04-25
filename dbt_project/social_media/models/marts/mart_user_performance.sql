SELECT
    u.user_id,
    u.username,
    u.display_name,
    u.location,
    u.account_type,
    u.influencer_tier,
    u.is_verified,
    u.follower_count,
    u.following_count,
    u.account_age_days,

    -- post metrics
    COUNT(DISTINCT p.post_id)              AS total_posts,
    SUM(p.like_count)                      AS total_likes_received,
    SUM(p.comment_count)                   AS total_comments_received,
    SUM(p.repost_count)                    AS total_reposts_received,
    SUM(p.view_count)                      AS total_views,
    ROUND(AVG(p.engagement_score), 4)      AS avg_engagement_score,
    COUNT(CASE WHEN p.is_viral
               THEN 1 END)                 AS viral_posts,

    -- activity metrics
    MIN(p.post_date)                       AS first_post_date,
    MAX(p.post_date)                       AS last_post_date,
    DATEDIFF('day',
        MIN(p.post_date),
        MAX(p.post_date))                  AS active_days_span,

    -- posts per day (activity rate)
    ROUND(
        CAST(COUNT(DISTINCT p.post_id)
             AS DOUBLE)
        / NULLIF(
            DATEDIFF('day',
                MIN(p.post_date),
                MAX(p.post_date)),
          0),
    2)                                     AS posts_per_day,

    CURRENT_TIMESTAMP                      AS built_at

FROM {{ ref('stg_users') }} u
LEFT JOIN {{ ref('stg_posts') }} p
       ON u.user_id = p.user_id
GROUP BY
    u.user_id, u.username, u.display_name,
    u.location, u.account_type, u.influencer_tier,
    u.is_verified, u.follower_count, u.following_count,
    u.account_age_days
ORDER BY total_likes_received DESC NULLS LAST