"""
Automated data quality checks for social media pipeline.
Runs against silver and gold layers in DuckDB.
"""
import duckdb
from pathlib import Path
from datetime import datetime

DB_PATH = Path(r"E:/de_project/social-media-pipeline/data/social_media.duckdb")
con     = duckdb.connect(str(DB_PATH))

def check(description: str, passed: bool, detail: str = "") -> bool:
    status = "OK" if passed else "FAIL"
    msg    = f"  [{status}]  {description}"
    if not passed and detail:
        msg += f"\n         {detail}"
    print(msg)
    return passed

def check_silver():
    print("\n── Silver layer ────────────────────────────────")
    results = []

    count = con.execute("SELECT COUNT(*) FROM main_silver.stg_users").fetchone()[0]
    results.append(check(f"stg_users has rows ({count})", count > 0))

    count = con.execute("SELECT COUNT(*) FROM main_silver.stg_posts").fetchone()[0]
    results.append(check(f"stg_posts has rows ({count})", count > 0))

    count = con.execute("SELECT COUNT(*) FROM main_silver.stg_comments").fetchone()[0]
    results.append(check(f"stg_comments has rows ({count})", count > 0))

    count = con.execute("SELECT COUNT(*) FROM main_silver.stg_likes").fetchone()[0]
    results.append(check(f"stg_likes has rows ({count})", count > 0))

    dupes = con.execute("SELECT COUNT(*) FROM (SELECT user_id FROM main_silver.stg_users GROUP BY user_id HAVING COUNT(*) > 1)").fetchone()[0]
    results.append(check("stg_users: no duplicate user_ids", dupes == 0, f"{dupes} duplicates found"))

    dupes = con.execute("SELECT COUNT(*) FROM (SELECT post_id FROM main_silver.stg_posts GROUP BY post_id HAVING COUNT(*) > 1)").fetchone()[0]
    results.append(check("stg_posts: no duplicate post_ids", dupes == 0, f"{dupes} duplicates found"))

    nulls = con.execute("SELECT COUNT(*) FROM main_silver.stg_posts WHERE engagement_score IS NULL").fetchone()[0]
    results.append(check("stg_posts: engagement_score not null", nulls == 0, f"{nulls} nulls found"))

    bad = con.execute("SELECT COUNT(*) FROM main_silver.stg_posts WHERE engagement_score < 0").fetchone()[0]
    results.append(check("stg_posts: engagement_score never negative", bad == 0))

    bad = con.execute("""
        SELECT COUNT(*) FROM main_silver.stg_posts
        WHERE post_type NOT IN ('text','image','video','link')
    """).fetchone()[0]
    results.append(check("stg_posts: post_type only valid values", bad == 0, f"{bad} invalid values"))

    bad = con.execute("""
        SELECT COUNT(*) FROM main_silver.stg_likes
        WHERE reaction_type NOT IN ('like','love','laugh','wow','sad')
    """).fetchone()[0]
    results.append(check("stg_likes: reaction_type only valid values", bad == 0, f"{bad} invalid values"))

    bad = con.execute("""
        SELECT COUNT(*) FROM main_silver.stg_comments
        WHERE sentiment NOT IN ('positive','neutral','negative')
    """).fetchone()[0]
    results.append(check("stg_comments: sentiment only valid values", bad == 0, f"{bad} invalid values"))

    return results

def check_gold():
    print("\n── Gold layer ──────────────────────────────────")
    results = []

    count = con.execute("SELECT COUNT(*) FROM main_gold.mart_daily_engagement").fetchone()[0]
    results.append(check(f"mart_daily_engagement has rows ({count})", count > 0))

    count = con.execute("SELECT COUNT(*) FROM main_gold.mart_hashtag_performance").fetchone()[0]
    results.append(check(f"mart_hashtag_performance has rows ({count})", count > 0))

    count = con.execute("SELECT COUNT(*) FROM main_gold.mart_user_performance").fetchone()[0]
    results.append(check(f"mart_user_performance has rows ({count})", count > 0))

    neg = con.execute("SELECT COUNT(*) FROM main_gold.mart_daily_engagement WHERE platform_engagement_rate < 0").fetchone()[0]
    results.append(check("mart_daily_engagement: engagement rate never negative", neg == 0))

    neg = con.execute("SELECT COUNT(*) FROM main_gold.mart_hashtag_performance WHERE trending_score < 0").fetchone()[0]
    results.append(check("mart_hashtag_performance: trending score never negative", neg == 0))

    dupes = con.execute("SELECT COUNT(*) FROM (SELECT date FROM main_gold.mart_daily_engagement GROUP BY date HAVING COUNT(*) > 1)").fetchone()[0]
    results.append(check("mart_daily_engagement: no duplicate dates", dupes == 0))

    dupes = con.execute("SELECT COUNT(*) FROM (SELECT hashtag FROM main_gold.mart_hashtag_performance GROUP BY hashtag HAVING COUNT(*) > 1)").fetchone()[0]
    results.append(check("mart_hashtag_performance: no duplicate hashtags", dupes == 0))

    return results

def run_all_checks():
    print(f"\n{'='*50}")
    print(f"  Data Quality Checks")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")

    silver = check_silver()
    gold   = check_gold()
    all_results = silver + gold

    passed = sum(1 for r in all_results if r)
    failed = len(all_results) - passed

    print(f"\n{'='*50}")
    if failed == 0:
        print(f"  ALL CHECKS PASSED ({passed}/{len(all_results)})")
    else:
        print(f"  SOME CHECKS FAILED ({passed}/{len(all_results)})")
        print(f"  Failed: {failed}")
    print(f"{'='*50}\n")
    return failed == 0

if __name__ == "__main__":
    passed = run_all_checks()
    exit(0 if passed else 1)