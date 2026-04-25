import json
import uuid
import random
from datetime import datetime, timedelta
from faker import Faker
from pathlib import Path
import pandas as pd
import duckdb

fake = Faker()
random.seed(99)

PROJECT_ROOT = Path(__file__).parent.parent
BRONZE_POSTS  = PROJECT_ROOT / "data" / "bronze" / "posts"
BRONZE_USERS  = PROJECT_ROOT / "data" / "bronze" / "users"

# Hashtag pool — simulates trending topics on a platform
HASHTAGS = [
    "tech", "ai", "python", "datascience", "machinelearning",
    "cloudcomputing", "programming", "opensource", "linux", "devops",
    "startup", "business", "entrepreneur", "marketing", "finance",
    "sports", "cricket", "football", "fitness", "health",
    "food", "travel", "photography", "art", "music",
    "news", "politics", "climate", "science", "space",
]

POST_TYPES = ["text", "text", "text", "image", "video", "link"]
LANGUAGES  = ["en", "en", "en", "ur", "hi", "de"]

def load_user_ids() -> list[str]:
    """Load existing user IDs so posts reference real users."""
    path = BRONZE_USERS / "users_snapshot.parquet"
    if not path.exists():
        raise FileNotFoundError(
            "Run generate_users.py first — posts need real user IDs"
        )
    con = duckdb.connect()
    return con.execute(
        f"SELECT user_id FROM read_parquet('{path}')"
    ).df()["user_id"].tolist()

def generate_post(user_ids: list[str]) -> dict:
    """
    Generates one social media post.

    Key design decisions:
    - hashtags stored as a JSON array string — demonstrates handling
      of semi-structured data, a very common real-world pattern
    - engagement_score is derived at ingest time as a raw signal
      (not cleaned — silver layer will normalise it)
    - post_date spread over 90 days to simulate timeline data
    """
    post_date = datetime.now() - timedelta(
        days=random.randint(0, 90),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )

    # Realistic engagement: most posts get very few interactions
    # viral posts are rare — power law again
    is_viral   = random.random() < 0.02   # 2% of posts go viral
    like_count = (
        random.randint(500, 50000) if is_viral
        else random.randint(0, 200)
    )
    comment_count = int(like_count * random.uniform(0.05, 0.3))
    repost_count  = int(like_count * random.uniform(0.01, 0.15))
    view_count    = like_count * random.randint(10, 100)

    # 1–3 hashtags per post
    num_tags = random.randint(0, 3)
    tags = random.sample(HASHTAGS, num_tags) if num_tags > 0 else []

    return {
        # identifiers
        "post_id":        str(uuid.uuid4()),
        "user_id":        random.choice(user_ids),

        # content
        "content":        fake.paragraph(nb_sentences=random.randint(1, 4)),
        "post_type":      random.choice(POST_TYPES),
        "language":       random.choice(LANGUAGES),
        "media_url":      fake.image_url() if random.random() > 0.6 else None,

        # hashtags as JSON array string
        # In silver layer we'll explode this into a separate table
        "hashtags":       json.dumps(tags),
        "hashtag_count":  len(tags),

        # engagement (raw counts at time of ingestion)
        "like_count":     like_count,
        "comment_count":  comment_count,
        "repost_count":   repost_count,
        "view_count":     view_count,

        # raw engagement score (silver will normalise 0-100)
        "raw_engagement_score": round(
            (like_count * 1.0 +
             comment_count * 2.0 +
             repost_count * 3.0) /
            max(view_count, 1) * 100, 4
        ),

        # flags
        "is_viral":       is_viral,
        "is_deleted":     random.random() < 0.02,  # 2% deleted posts
        "is_nsfw":        random.random() < 0.01,

        # location (optional)
        "geo_country":    random.choice([None, None, "US", "PK", "IN", "UK"]),

        # timestamps
        "post_date":      post_date.strftime("%Y-%m-%dT%H:%M:%S"),
        "ingested_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "source_system":  "post_service_v3",
    }

def generate_posts(user_ids: list[str], n: int = 2000) -> list[dict]:
    return [generate_post(user_ids) for _ in range(n)]

def save_posts(posts: list[dict], batch_id: str) -> Path:
    """
    Posts are append-only fact data — partitioned by date like project 1.
    Every ingestion run adds a new batch file.
    """
    now = datetime.now()
    partition = (
        BRONZE_POSTS
        / f"year={now.year}"
        / f"month={now.month:02d}"
        / f"day={now.day:02d}"
    )
    partition.mkdir(parents=True, exist_ok=True)

    # Save JSONL
    jsonl_path = partition / f"batch_{batch_id}.jsonl"
    with open(jsonl_path, "w") as f:
        for post in posts:
            f.write(json.dumps(post) + "\n")

    # Save Parquet
    df = pd.DataFrame(posts)
    df["post_date"]   = pd.to_datetime(df["post_date"])
    df["ingested_at"] = pd.to_datetime(df["ingested_at"])
    df.to_parquet(
        partition / f"batch_{batch_id}.parquet",
        index=False, engine="pyarrow"
    )
    return jsonl_path

if __name__ == "__main__":
    print("Loading user IDs...")
    user_ids = load_user_ids()
    print(f"  {len(user_ids)} users loaded")

    print("Generating posts...")
    posts    = generate_posts(user_ids, n=2000)
    batch_id = str(uuid.uuid4())[:8]
    path     = save_posts(posts, batch_id)

    print(f"  {len(posts)} posts saved → {path}")
    print(f"  Viral posts       : {sum(p['is_viral'] for p in posts)}")
    print(f"  Posts with tags   : {sum(p['hashtag_count'] > 0 for p in posts)}")
    print(f"  Post types:")
    from collections import Counter
    for k, v in Counter(p['post_type'] for p in posts).items():
        print(f"    {k:<10} {v}")