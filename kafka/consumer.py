import json
import logging
import signal
from datetime import datetime
from pathlib import Path
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("kafka-consumer")

KAFKA_BROKER = "localhost:9092"
PROJECT_ROOT = Path(__file__).parent.parent
TOPICS       = ["social.posts", "social.likes", "social.comments"]

class BronzeWriter:
    FLUSH_EVERY = 50

    def __init__(self):
        self.buffers       = {"posts": [], "likes": [], "comments": []}
        self.total_written = {"posts": 0,  "likes": 0,  "comments": 0}

    def _entity(self, topic: str) -> str:
        return topic.split(".")[1]

    def add(self, topic: str, event: dict):
        entity = self._entity(topic)
        event["consumed_at"] = datetime.now().isoformat()
        self.buffers[entity].append(event)
        total = sum(len(b) for b in self.buffers.values())
        if total >= self.FLUSH_EVERY:
            self.flush()

    def flush(self):
        now     = datetime.now()
        flushed = 0
        for entity, events in self.buffers.items():
            if not events:
                continue
            path = (
                PROJECT_ROOT / "data" / "bronze" / entity
                / f"year={now.year}"
                / f"month={now.month:02d}"
                / f"day={now.day:02d}"
            )
            path.mkdir(parents=True, exist_ok=True)
            ts   = now.strftime("%H%M%S%f")
            file = path / f"stream_{ts}.jsonl"
            with open(file, "a") as f:
                for ev in events:
                    f.write(json.dumps(ev) + "\n")
            self.total_written[entity] += len(events)
            flushed += len(events)
            self.buffers[entity] = []
        if flushed > 0:
            log.info(
                f"  💾 Flushed {flushed} events | "
                f"posts={self.total_written['posts']} "
                f"likes={self.total_written['likes']} "
                f"comments={self.total_written['comments']}"
            )

    def flush_all(self):
        self.flush()
        log.info(
            f"Final totals — posts={self.total_written['posts']} "
            f"likes={self.total_written['likes']} "
            f"comments={self.total_written['comments']}"
        )


def run_consumer():
    log.info("="*50)
    log.info("  Kafka Consumer Starting")
    log.info("="*50)

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BROKER,
        "group.id":           "social-media-bronze-writer",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe(TOPICS)
    writer  = BronzeWriter()
    total   = 0
    running = True

    def shutdown(sig, frame):
        nonlocal running
        log.info("Shutting down...")
        running = False

    signal.signal(signal.SIGINT, shutdown)
    log.info(f"Subscribed to: {TOPICS}\nWaiting for messages...\n")

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                writer.flush()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode("utf-8"))
            writer.add(msg.topic(), event)
            total += 1

            log.info(
                f"  📥 {msg.topic():<20} "
                f"| partition={msg.partition()} "
                f"| offset={msg.offset():>6} "
                f"| event={event.get('event_type','?')}"
            )

    finally:
        writer.flush_all()
        consumer.close()
        log.info(f"Total consumed: {total}")


if __name__ == "__main__":
    run_consumer()