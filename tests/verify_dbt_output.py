import duckdb
from pathlib import Path

DB  = "E:/Data engineering project/social-media-pipeline/data/social_media.duckdb"
con = duckdb.connect(DB)

print("\n" + "="*55)
print("  dbt Output Verification")
print("="*55)

print("\n  Silver — stg_users:")
print(con.execute("""
    SELECT COUNT(*) AS rows,
           COUNT(DISTINCT influencer_tier) AS tiers,
           SUM(CAST(is_influencer AS INT)) AS influencers
    FROM main_silver.stg_users
""").df().to_string(index=False))

print("\n  Silver — stg_posts:")
print(con.execute("""
    SELECT COUNT(*) AS rows,
           COUNT(DISTINCT engagement_tier) AS eng_tiers,
           SUM(CAST(is_viral AS INT)) AS viral,
           SUM(CASE WHEN dq_flag != 'ok' THEN 1 ELSE 0 END) AS flagged
    FROM main_silver.stg_posts
""").df().to_string(index=False))

print("\n  Gold — daily engagement (top 5 days):")
print(con.execute("""
    SELECT date, total_posts, total_likes,
           viral_posts, platform_engagement_rate
    FROM main_gold.mart_daily_engagement
    ORDER BY total_likes DESC
    LIMIT 5
""").df().to_string(index=False))

print("\n  Gold — top 10 trending hashtags:")
print(con.execute("""
    SELECT hashtag, total_posts, total_likes, trending_score
    FROM main_gold.mart_hashtag_performance
    ORDER BY trending_score DESC
    LIMIT 10
""").df().to_string(index=False))

print("\n  Gold — top 5 users by engagement:")
print(con.execute("""
    SELECT username, influencer_tier, total_posts,
           total_likes_received, viral_posts
    FROM main_gold.mart_user_performance
    ORDER BY total_likes_received DESC
    LIMIT 5
""").df().to_string(index=False))

print("\n" + "="*55)
print("  ✅ dbt output verified")
print("="*55 + "\n")