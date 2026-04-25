import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
con = duckdb.connect()

print("\n" + "="*55)
print("  Bronze Layer Summary")
print("="*55)

# Users
users = con.execute(f"""
    SELECT
        COUNT(*)                            AS total_users,
        SUM(CAST(is_verified AS INT))       AS verified,
        COUNT(DISTINCT account_type)        AS account_types,
        COUNT(DISTINCT location)            AS countries
    FROM read_parquet('{PROJECT_ROOT}/data/bronze/users/users_snapshot.parquet')
""").df()
print("\n  Users:")
print(users.to_string(index=False))

# Posts
posts = con.execute(f"""
    SELECT
        COUNT(*)                            AS total_posts,
        SUM(CAST(is_viral AS INT))          AS viral_posts,
        COUNT(DISTINCT post_type)           AS post_types,
        ROUND(AVG(like_count), 1)           AS avg_likes,
        ROUND(AVG(raw_engagement_score), 4) AS avg_engagement
    FROM read_parquet('{PROJECT_ROOT}/data/bronze/posts/**/*.parquet')
""").df()
print("\n  Posts:")
print(posts.to_string(index=False))

# Comments
comments = con.execute(f"""
    SELECT
        COUNT(*)                    AS total_comments,
        COUNT(DISTINCT post_id)     AS unique_posts_commented,
        COUNT(DISTINCT sentiment)   AS sentiments
    FROM read_parquet('{PROJECT_ROOT}/data/bronze/comments/**/*.parquet')
""").df()
print("\n  Comments:")
print(comments.to_string(index=False))

# Likes
likes = con.execute(f"""
    SELECT
        COUNT(*)                    AS total_likes,
        COUNT(DISTINCT post_id)     AS unique_posts_liked,
        COUNT(DISTINCT reaction_type) AS reaction_types
    FROM read_parquet('{PROJECT_ROOT}/data/bronze/likes/**/*.parquet')
""").df()
print("\n  Likes:")
print(likes.to_string(index=False))

# Top hashtags
print("\n  Top 10 hashtags:")
hashtags = con.execute(f"""
    SELECT
        hashtag,
        COUNT(*) AS usage_count
    FROM (
        SELECT UNNEST(
            FROM_JSON(hashtags, '["varchar"]')
        ) AS hashtag
        FROM read_parquet('{PROJECT_ROOT}/data/bronze/posts/**/*.parquet')
        WHERE hashtag_count > 0
    )
    GROUP BY hashtag
    ORDER BY usage_count DESC
    LIMIT 10
""").df()
print(hashtags.to_string(index=False))

print("\n" + "="*55)
print("  ✅ Bronze layer verified")
print("="*55 + "\n")