import json
import uuid
import random
import time
import logging
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer
from pathlib import Path
import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("kafka-producer")

fake = Faker()
random.seed(None)

KAFKA_BROKER = "localhost:9092"

HASHTAGS = [
    "tech", "ai", "python", "datascience", "machinelearning",
    "cloudcomputing", "devops", "startup", "business", "sports",
    "cricket", "football", "fitness", "food", "travel",
    "news", "politics", "climate", "science", "space",
]

def load_user_ids() -> list[str]:
    project_root = Path(__file__).parent.parent
    db_path = project_root / "data" / "social_media.duckdb"
    con = duckdb.connect(str(db_path))
    users = con.execute(
        "SELECT user_id FROM main_silver.stg_users LIMIT 1000"
    ).df()["user_id"].tolist()
    con.close()
    log.info(f"Loaded {len(users)} user IDs")
    return users

def make_post_event(user_ids: list) -> dict:
    tags = random.sample(HASHTAGS, random.randint(0, 3))
    return {
        "event_type":    "post_published",
        "post_id":       str(uuid.uuid4()),
        "user_id":       random.choice(user_ids),
        "content":       fake.paragraph(nb_sentences=random.randint(1, 3)),
        "post_type":     random.choices(
                             ["text", "image", "video", "link"],
                             weights=[60, 20, 10, 10]
                         )[0],
        "hashtags":      json.dumps(tags),
        "hashtag_count": len(tags),
        "language":      random.choice(["en", "en", "en", "ur", "hi"]),
        "timestamp":     datetime.now().isoformat(),
        "event_id":      str(uuid.uuid4()),
    }

def make_like_event(user_ids: list, post_ids: list) -> dict:
    return {
        "event_type":    "post_liked",
        "like_id":       str(uuid.uuid4()),
        "post_id":       random.choice(post_ids) if post_ids
                         else str(uuid.uuid4()),
        "user_id":       random.choice(user_ids),
        "reaction_type": random.choices(
                             ["like", "love", "laugh", "wow", "sad"],
                             weights=[60, 20, 10, 5, 5]
                         )[0],
        "timestamp":     datetime.now().isoformat(),
        "event_id":      str(uuid.uuid4()),
    }

def make_comment_event(user_ids: list, post_ids: list) -> dict:
    return {
        "event_type":  "comment_posted",
        "comment_id":  str(uuid.uuid4()),
        "post_id":     random.choice(post_ids) if post_ids
                       else str(uuid.uuid4()),
        "user_id":     random.choice(user_ids),
        "content":     fake.sentence(nb_words=random.randint(3, 15)),
        "sentiment":   random.choices(
                           ["positive", "neutral", "negative"],
                           weights=[50, 35, 15]
                       )[0],
        "timestamp":   datetime.now().isoformat(),
        "event_id":    str(uuid.uuid4()),
    }

def delivery_report(err, msg):
    if err:
        log.error(f"Delivery failed: {err}")

def run_producer(events_per_second: float = 3.0, max_events: int = 300):
    log.info("="*50)
    log.info("  Kafka Producer Starting")
    log.info(f"  Rate: {events_per_second} events/second")
    log.info(f"  Max events: {max_events}")
    log.info("="*50)

    user_ids = load_user_ids()
    post_ids = []

    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "acks": "all",
        "retries": 3,
        "linger.ms": 10,
    })

    log.info("Connected to Kafka broker")

    total_sent = 0
    interval   = 1.0 / events_per_second

    try:
        while total_sent < max_events:

            event_type = random.choices(
                ["post", "like", "comment"],
                weights=[10, 70, 20]
            )[0]

            if event_type == "post":
                event = make_post_event(user_ids)
                post_ids.append(event["post_id"])
                if len(post_ids) > 1000:
                    post_ids = post_ids[-1000:]
                producer.produce(
                    "social.posts",
                    key=event["post_id"],
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report
                )
                log.info(
                    f"  📤 POST    | user={event['user_id'][:8]}... "
                    f"| tags={event['hashtag_count']} "
                    f"| type={event['post_type']}"
                )

            elif event_type == "like":
                event = make_like_event(user_ids, post_ids)
                producer.produce(
                    "social.likes",
                    key=event["post_id"],
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report
                )
                log.info(
                    f"  ❤  LIKE    | user={event['user_id'][:8]}... "
                    f"| reaction={event['reaction_type']}"
                )

            else:
                event = make_comment_event(user_ids, post_ids)
                producer.produce(
                    "social.comments",
                    key=event["post_id"],
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report
                )
                log.info(
                    f"  💬 COMMENT | user={event['user_id'][:8]}... "
                    f"| sentiment={event['sentiment']}"
                )

            total_sent += 1
            producer.poll(0)  # trigger delivery callbacks
            time.sleep(interval)

    except KeyboardInterrupt:
        log.info("\nProducer stopped by user")
    finally:
        producer.flush()
        log.info(f"\nTotal events sent: {total_sent}")


if __name__ == "__main__":
    run_producer(events_per_second=3.0, max_events=300)