from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()
import os
import time
from kafka_io import get_consumer
from base import upsert, publish_event, Post

TOPIC = os.environ.get("KAFKA_TOPIC", "raw.posts")


def _flush(batch):
    if not batch:
        return 0
    n = upsert(batch)
    publish_event({"event": "new_posts", "count": n})
    print(f"[consumer] upserted={n}")
    return n


def main():
    consumer = get_consumer()
    consumer.subscribe([TOPIC])
    print(f"[consumer] consuming '{TOPIC}' ... (Ctrl+C to stop)")
    batch = []
    last = time.time()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if batch and time.time() - last > 1.0:
                    _flush(batch)
                    batch = []
                    last = time.time()
                continue
            if msg.error():
                print("[consumer] error:", msg.error())
                continue
            try:
                batch.append(Post.from_json(msg.value()))
            except Exception as e:
                print("[consumer] parse error:", e)
            if len(batch) >= 50:
                _flush(batch)
                batch = []
                last = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        _flush(batch)
        consumer.close()


if __name__ == "__main__":
    main()
