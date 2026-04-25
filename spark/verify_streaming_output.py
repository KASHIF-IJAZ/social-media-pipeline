"""
Verifies PySpark streaming output in gold layer.
"""
import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
GOLD_DIR     = PROJECT_ROOT / "data" / "gold" / "streaming"
con          = duckdb.connect()

print("\n" + "="*55)
print("  Spark Streaming Output Verification")
print("="*55)

# Trending hashtags
trending_path = GOLD_DIR / "trending_hashtags"
if list(trending_path.glob("**/*.parquet")):
    print("\n  Trending hashtags (top 10):")
    print(con.execute(f"""
        SELECT hashtag,
               SUM(post_count)     AS total_mentions,
               COUNT(*)            AS window_count,
               MIN(window_start)   AS first_window,
               MAX(window_end)     AS last_window
        FROM read_parquet('{trending_path}/**/*.parquet')
        GROUP BY hashtag
        ORDER BY total_mentions DESC
        LIMIT 10
    """).df().to_string(index=False))
else:
    print("\n  No trending hashtag data yet")

# Reaction counts
reaction_path = GOLD_DIR / "reaction_counts"
if list(reaction_path.glob("**/*.parquet")):
    print("\n  Reaction counts:")
    print(con.execute(f"""
        SELECT reaction_type,
               SUM(reaction_count) AS total_reactions
        FROM read_parquet('{reaction_path}/**/*.parquet')
        GROUP BY reaction_type
        ORDER BY total_reactions DESC
    """).df().to_string(index=False))
else:
    print("\n  No reaction data yet")

# Sentiment
sentiment_path = GOLD_DIR / "comment_sentiment"
if list(sentiment_path.glob("**/*.parquet")):
    print("\n  Comment sentiment:")
    print(con.execute(f"""
        SELECT sentiment,
               SUM(comment_count) AS total_comments
        FROM read_parquet('{sentiment_path}/**/*.parquet')
        GROUP BY sentiment
        ORDER BY total_comments DESC
    """).df().to_string(index=False))
else:
    print("\n  No sentiment data yet")

print("\n" + "="*55)
print("  ✅ Verification complete")
print("="*55 + "\n")