import json
import uuid
import random
from datetime import datetime, timedelta
from faker import Faker
from pathlib import Path
import pandas as pd
import duckdb

fake = Faker()
PROJECT_ROOT   = Path(__file__).parent.parent
BRONZE_COMMENTS = PROJECT_ROOT / "data" / "bronze" / "comments"
BRONZE_LIKES    = PROJECT_ROOT / "data" / "bronze" / "likes"

SENTIMENT = ["positive", "positive", "neutral", "neutral", "negative"]

def load_post_and_user_ids() -> tuple[list, list]:
    con = duckdb.connect()
    posts = con.execute(f"""
        SELECT post_id, post_date FROM read_parquet(
            '{PROJECT_ROOT}/data/bronze/posts/**/*.parquet'
        )
    """).df()
    users = con.execute(f"""
        SELECT user_id FROM read_parquet(
            '{PROJECT_ROOT}/data/bronze/users/users_snapshot.parquet'
        )
    """).df()
    return (
        list(zip(posts["post_id"], posts["post_date"])),
        users["user_id"].tolist()
    )

def generate_comment(post_id: str, post_date, user_ids: list) -> dict:
    """
    Comments always happen AFTER the post — important constraint.
    In real pipelines you always validate temporal consistency.
    """
    if isinstance(post_date, str):
        post_dt = datetime.strptime(post_date, "%Y-%m-%dT%H:%M:%S")
    else:
        post_dt = post_date.to_pydatetime()

    # Comment must be after post date
    commented_at = post_dt + timedelta(
        minutes=random.randint(1, 60 * 24 * 7)  # within a week
    )
    # But not in the future
    if commented_at > datetime.now():
        commented_at = datetime.now() - timedelta(minutes=random.randint(1, 60))

    return {
        "comment_id":    str(uuid.uuid4()),
        "post_id":       post_id,
        "user_id":       random.choice(user_ids),
        "content":       fake.sentence(nb_words=random.randint(3, 20)),
        "like_count":    random.randint(0, 500),
        "sentiment":     random.choice(SENTIMENT),
        "is_deleted":    random.random() < 0.03,
        "commented_at":  commented_at.strftime("%Y-%m-%dT%H:%M:%S"),
        "ingested_at":   datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "source_system": "comment_service_v1",
    }

def generate_like(post_id: str, post_date, user_ids: list) -> dict:
    """Like events — simplest interaction, but highest volume."""
    if isinstance(post_date, str):
        post_dt = datetime.strptime(post_date, "%Y-%m-%dT%H:%M:%S")
    else:
        post_dt = post_date.to_pydatetime()

    liked_at = post_dt + timedelta(
        minutes=random.randint(1, 60 * 24 * 30)
    )
    if liked_at > datetime.now():
        liked_at = datetime.now() - timedelta(minutes=random.randint(1, 60))

    return {
        "like_id":       str(uuid.uuid4()),
        "post_id":       post_id,
        "user_id":       random.choice(user_ids),
        "reaction_type": random.choices(
                             ["like", "love", "laugh", "wow", "sad"],
                             weights=[60, 20, 10, 5, 5]
                         )[0],
        "liked_at":      liked_at.strftime("%Y-%m-%dT%H:%M:%S"),
        "ingested_at":   datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "source_system": "reaction_service_v1",
    }

def save_jsonl_and_parquet(records: list, folder: Path, name: str) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    now      = datetime.now()
    batch_id = str(uuid.uuid4())[:8]
    partition = (
        folder
        / f"year={now.year}"
        / f"month={now.month:02d}"
        / f"day={now.day:02d}"
    )
    partition.mkdir(parents=True, exist_ok=True)

    jsonl_path = partition / f"{name}_{batch_id}.jsonl"
    with open(jsonl_path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    df = pd.DataFrame(records)
    df.to_parquet(
        partition / f"{name}_{batch_id}.parquet",
        index=False, engine="pyarrow"
    )
    return jsonl_path

if __name__ == "__main__":
    print("Loading post and user IDs...")
    post_pairs, user_ids = load_post_and_user_ids()
    print(f"  {len(post_pairs)} posts, {len(user_ids)} users loaded")

    print("\nGenerating comments...")
    comments = []
    # ~3 comments per post on average
    for post_id, post_date in random.choices(post_pairs, k=6000):
        comments.append(generate_comment(post_id, post_date, user_ids))
    path = save_jsonl_and_parquet(comments, BRONZE_COMMENTS, "comments")
    print(f"  {len(comments)} comments → {path}")

    print("\nGenerating likes...")
    likes = []
    # ~15 likes per post on average
    for post_id, post_date in random.choices(post_pairs, k=30000):
        likes.append(generate_like(post_id, post_date, user_ids))
    path = save_jsonl_and_parquet(likes, BRONZE_LIKES, "likes")
    print(f"  {len(likes)} likes → {path}")