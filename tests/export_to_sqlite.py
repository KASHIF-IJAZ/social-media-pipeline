"""
Exports all gold layer tables to SQLite for Metabase dashboard.
Combines batch gold (dbt output) and streaming gold (Phase 4 output).
"""
import sqlite3
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(r"E:/de_project/social-media-pipeline")
DB_PATH      = PROJECT_ROOT / "data" / "social_media.duckdb"
GOLD_DIR     = PROJECT_ROOT / "data" / "gold"
STREAMING    = GOLD_DIR / "streaming"
SQLITE_PATH  = PROJECT_ROOT / "data" / "dashboard.db"

con    = duckdb.connect(str(DB_PATH))
sqlite = sqlite3.connect(str(SQLITE_PATH))

print("\n" + "="*55)
print("  Exporting Gold Layer to SQLite")
print("="*55)

def fix_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == "object":
            continue
        try:
            if hasattr(df[col].dtype, "tz"):
                df[col] = df[col].dt.tz_localize(None)
            if str(df[col].dtype).startswith("datetime"):
                df[col] = df[col].astype(str)
        except Exception:
            pass
    return df

def export_table(df: pd.DataFrame, name: str):
    df = fix_timestamps(df)
    df.to_sql(name, sqlite, if_exists="replace", index=False)
    print(f"  ✅ {name}: {len(df)} rows")

# ── dbt gold tables ────────────────────────────────────────────
print("\n  dbt gold tables:")

df = con.execute("SELECT * FROM main_gold.mart_daily_engagement").df()
export_table(df, "daily_engagement")

df = con.execute("SELECT * FROM main_gold.mart_hashtag_performance").df()
export_table(df, "hashtag_performance")

df = con.execute("SELECT * FROM main_gold.mart_user_performance").df()
export_table(df, "user_performance")

# ── Streaming gold tables ──────────────────────────────────────
print("\n  Streaming gold tables:")

for name in ["trending_hashtags", "reaction_counts", "comment_sentiment"]:
    path = STREAMING / name
    if path.exists() and list(path.glob("**/*.parquet")):
        try:
            df = duckdb.connect().execute(f"""
                SELECT * FROM read_parquet(
                    '{str(path).replace(chr(92), '/')}/**/*.parquet',
                    union_by_name=true
                )
            """).df()
            export_table(df, f"streaming_{name}")
        except Exception as e:
            print(f"  ⚠ {name}: {e}")
    else:
        print(f"  ⚠ {name}: no data found")

# ── Summary views ──────────────────────────────────────────────
print("\n  Creating summary views...")

sqlite.execute("DROP VIEW IF EXISTS platform_kpis")
sqlite.execute("""
    CREATE VIEW platform_kpis AS
    SELECT
        ROUND(SUM(revenue), 2)              AS total_revenue,
        SUM(total_posts)                    AS total_posts,
        SUM(completed_orders)               AS completed_orders,
        ROUND(AVG(avg_order_value), 2)      AS avg_order_value,
        SUM(unique_customers)               AS total_customers
    FROM daily_engagement
""")

sqlite.execute("DROP VIEW IF EXISTS top_hashtags")
sqlite.execute("""
    CREATE VIEW top_hashtags AS
    SELECT hashtag, trending_score, total_posts, total_likes
    FROM hashtag_performance
    ORDER BY trending_score DESC
    LIMIT 20
""")

sqlite.commit()
sqlite.close()

print(f"\n  ✅ SQLite database ready: {SQLITE_PATH}")
print("="*55 + "\n")