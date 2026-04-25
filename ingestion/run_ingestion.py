import time
import logging
from pathlib import Path
from datetime import datetime
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("social-pipeline")

def run_task(name: str, func, retries: int = 2):
    for attempt in range(1, retries + 2):
        try:
            log.info(f"  ┌─ {name}  (attempt {attempt})")
            result = func()
            log.info(f"  └─ ✅ {name} done")
            return result
        except Exception as e:
            log.error(f"  └─ ❌ {name} failed: {e}")
            if attempt <= retries:
                log.info(f"     Retrying in 3s...")
                time.sleep(3)
            else:
                raise

def main():
    log.info("=" * 55)
    log.info("  Social Media Pipeline — Phase 1 Ingestion")
    log.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    def ingest_users():
        from ingestion.generate_users import generate_users, save_users
        users = generate_users(1000)
        save_users(users)
        log.info(f"     1000 users saved")

    def ingest_posts():
        from ingestion.generate_posts import (
            load_user_ids, generate_posts, save_posts
        )
        import uuid
        user_ids = load_user_ids()
        posts    = generate_posts(user_ids, n=2000)
        save_posts(posts, str(uuid.uuid4())[:8])
        log.info(f"     2000 posts saved")

    def ingest_interactions():
        from ingestion.generate_interactions import (
            load_post_and_user_ids,
            generate_comment, generate_like,
            save_jsonl_and_parquet
        )
        import random
        from pathlib import Path
        post_pairs, user_ids = load_post_and_user_ids()
        project_root = Path(__file__).parent.parent

        comments = [
            generate_comment(pid, pdate, user_ids)
            for pid, pdate in random.choices(post_pairs, k=6000)
        ]
        save_jsonl_and_parquet(
            comments,
            project_root / "data" / "bronze" / "comments",
            "comments"
        )

        likes = [
            generate_like(pid, pdate, user_ids)
            for pid, pdate in random.choices(post_pairs, k=30000)
        ]
        save_jsonl_and_parquet(
            likes,
            project_root / "data" / "bronze" / "likes",
            "likes"
        )
        log.info(f"     6000 comments + 30000 likes saved")

    run_task("ingest_users",        ingest_users)
    run_task("ingest_posts",        ingest_posts)
    run_task("ingest_interactions", ingest_interactions)

    log.info("=" * 55)
    log.info("  ✅ Phase 1 ingestion complete")
    log.info("=" * 55)

if __name__ == "__main__":
    main()