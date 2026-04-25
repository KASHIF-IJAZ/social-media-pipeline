import json
import uuid
import random
from datetime import datetime, timedelta
from faker import Faker
from pathlib import Path

fake = Faker()
random.seed(42)

PROJECT_ROOT = Path(__file__).parent.parent
BRONZE_USERS = PROJECT_ROOT / "data" / "bronze" / "users"

# Platform categories — makes data more realistic
ACCOUNT_TYPES  = ["personal", "business", "creator", "news"]
ACCOUNT_STATUS = ["active", "active", "active", "suspended", "inactive"]
COUNTRIES = [
    "United States", "Pakistan", "India", "United Kingdom",
    "Canada", "Germany", "Australia", "Brazil", "Turkey", "UAE"
]

def generate_user() -> dict:
    """
    Generates one realistic social media user profile.

    Key design decisions:
    - joined_at spread over 5 years → simulates platform growth
    - follower_count uses power law distribution → most users have few followers,
      a few have millions. This is exactly how real social platforms work.
    - is_verified weighted low → only ~3% of users are verified (realistic)
    """
    joined_at = datetime.now() - timedelta(
        days=random.randint(0, 5 * 365)
    )

    # Power law for follower count (realistic social media distribution)
    follower_count = int(random.paretovariate(1.5) * 100)
    follower_count = min(follower_count, 10_000_000)  # cap at 10M

    following_count = random.randint(0, min(follower_count * 3 + 50, 5000))

    return {
        # identifiers
        "user_id":          str(uuid.uuid4()),
        "username":         fake.user_name(),
        "display_name":     fake.name(),
        "email":            fake.email(),

        # profile
        "bio":              fake.sentence(nb_words=10),
        "location":         random.choice(COUNTRIES),
        "website":          fake.url() if random.random() > 0.6 else None,
        "profile_image_url": fake.image_url(),

        # account metadata
        "account_type":     random.choice(ACCOUNT_TYPES),
        "account_status":   random.choices(
                                ACCOUNT_STATUS,
                                weights=[60, 20, 10, 5, 5]
                            )[0],
        "is_verified":      random.random() < 0.03,   # 3% verified
        "language":         random.choice(["en", "ur", "hi", "de", "fr", "pt"]),

        # engagement stats
        "follower_count":   follower_count,
        "following_count":  following_count,
        "post_count":       random.randint(0, 5000),

        # timestamps
        "joined_at":        joined_at.strftime("%Y-%m-%dT%H:%M:%S"),
        "last_active_at":   (
                                joined_at + timedelta(
                                    days=random.randint(0, 365)
                                )
                            ).strftime("%Y-%m-%dT%H:%M:%S"),

        # pipeline metadata
        "ingested_at":      datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "source_system":    "user_service_v2",
    }

def generate_users(n: int = 1000) -> list[dict]:
    return [generate_user() for _ in range(n)]

def save_users(users: list[dict]) -> Path:
    """
    Users are reference data — saved as a single snapshot file.
    Unlike posts (append-only), user profiles replace the previous snapshot.
    This is a common pattern: fact tables append, dimension tables replace.
    """
    BRONZE_USERS.mkdir(parents=True, exist_ok=True)
    path = BRONZE_USERS / "users_snapshot.jsonl"

    with open(path, "w") as f:
        for user in users:
            f.write(json.dumps(user) + "\n")

    # Also save as parquet for fast querying
    import pandas as pd
    df = pd.DataFrame(users)
    df["joined_at"]      = pd.to_datetime(df["joined_at"])
    df["last_active_at"] = pd.to_datetime(df["last_active_at"])
    df["ingested_at"]    = pd.to_datetime(df["ingested_at"])
    df.to_parquet(
        BRONZE_USERS / "users_snapshot.parquet",
        index=False, engine="pyarrow"
    )
    return path

if __name__ == "__main__":
    print("Generating users...")
    users = generate_users(1000)
    path  = save_users(users)
    print(f"  {len(users)} users saved → {path}")
    print(f"  Verified accounts : {sum(u['is_verified'] for u in users)}")
    print(f"  Account types     : {{}}")
    from collections import Counter
    for k, v in Counter(u['account_type'] for u in users).items():
        print(f"    {k:<12} {v}")