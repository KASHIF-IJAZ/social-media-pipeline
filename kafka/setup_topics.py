from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
import time

KAFKA_BROKER = "localhost:9092"

TOPICS = [
    NewTopic("social.posts",    num_partitions=3, replication_factor=1),
    NewTopic("social.likes",    num_partitions=3, replication_factor=1),
    NewTopic("social.comments", num_partitions=3, replication_factor=1),
    NewTopic("social.users",    num_partitions=1, replication_factor=1),
]

def create_topics(retries: int = 5):
    for attempt in range(1, retries + 1):
        try:
            admin = AdminClient({"bootstrap.servers": KAFKA_BROKER})
            # Test connection by listing topics
            admin.list_topics(timeout=5)
            print(f"Connected to Kafka at {KAFKA_BROKER}")
            break
        except Exception as e:
            print(f"Attempt {attempt}/{retries} — Kafka not ready: {e}")
            if attempt == retries:
                raise
            time.sleep(5)

    fs = admin.create_topics(TOPICS)

    for topic, f in fs.items():
        try:
            f.result()
            print(f"  ✅ Created topic: {topic}")
        except KafkaException as e:
            if "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"  ⏭  Already exists: {topic}")
            else:
                print(f"  ❌ Failed: {topic} — {e}")

if __name__ == "__main__":
    print("="*50)
    print("  Setting up Kafka topics")
    print("="*50 + "\n")
    create_topics()
    print("\n✅ All topics ready")