import json
import logging
from datetime import datetime
from pathlib import Path
from collections import defaultdict
import duckdb
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger()

BRONZE_DIR = Path(r"E:/de_project/social-media-pipeline/data/bronze")
GOLD_DIR   = Path(r"E:/de_project/social-media-pipeline/data/gold/streaming")
GOLD_DIR.mkdir(parents=True, exist_ok=True)

log.info("BRONZE_DIR: " + str(BRONZE_DIR))
log.info("Exists: " + str(BRONZE_DIR.exists()))

con = duckdb.connect()
events = {}

for entity in ["posts", "likes", "comments"]:
    path  = BRONZE_DIR / entity
    files = list(path.glob("**/*.parquet")) if path.exists() else []
    log.info(entity + ": " + str(len(files)) + " files found")
    if files:
        try:
            query = "SELECT * FROM read_parquet('" + str(path).replace("\\", "/") + "/**/*.parquet', union_by_name=true) LIMIT 2000"
            df = con.execute(query).df()
            events[entity] = df.to_dict("records")
            log.info(entity + ": " + str(len(events[entity])) + " records loaded")
        except Exception as e:
            log.error(entity + " error: " + str(e))
            events[entity] = []
    else:
        events[entity] = []

total = sum(len(v) for v in events.values())
log.info("Total records: " + str(total))

if total == 0:
    log.error("No data found. Check bronze layer.")
else:
    hashtag_counts   = defaultdict(int)
    reaction_counts  = defaultdict(int)
    sentiment_counts = defaultdict(int)

    for post in events.get("posts", []):
        try:
            for tag in json.loads(post.get("hashtags", "[]")):
                if tag.strip():
                    hashtag_counts[tag.strip()] += 1
        except Exception:
            pass

    for like in events.get("likes", []):
        reaction_counts[like.get("reaction_type", "like")] += 1

    for comment in events.get("comments", []):
        sentiment_counts[comment.get("sentiment", "neutral")] += 1

    now = datetime.now()

    log.info("=" * 50)
    log.info("posts=" + str(len(events["posts"])) +
             " likes=" + str(len(events["likes"])) +
             " comments=" + str(len(events["comments"])))

    if hashtag_counts:
        top5 = sorted(hashtag_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        log.info("Top hashtags: " + ", ".join(t + "(" + str(c) + ")" for t, c in top5))

    if reaction_counts:
        top = sorted(reaction_counts.items(), key=lambda x: x[1], reverse=True)[0]
        log.info("Top reaction: " + top[0] + " x" + str(top[1]))

    if sentiment_counts:
        top = sorted(sentiment_counts.items(), key=lambda x: x[1], reverse=True)[0]
        log.info("Top sentiment: " + top[0] + " x" + str(top[1]))

    for name, data, c1, c2 in [
        ("trending_hashtags",  hashtag_counts,   "hashtag",       "post_count"),
        ("reaction_counts",    reaction_counts,  "reaction_type", "reaction_count"),
        ("comment_sentiment",  sentiment_counts, "sentiment",     "comment_count"),
    ]:
        if data:
            df  = pd.DataFrame([{c1: k, c2: v, "window_end": now} for k, v in data.items()])
            out = GOLD_DIR / name
            out.mkdir(parents=True, exist_ok=True)
            df.to_parquet(str(out / "batch_0001.parquet"), index=False)
            log.info("Saved " + name + ": " + str(len(df)) + " rows")

    log.info("=" * 50)
    log.info("Done! Results in: " + str(GOLD_DIR))