from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()
import os
from kafka_io import get_producer
from hackernews import HackerNews
from reddit import Reddit
from bluesky import BlueskyFirehose

TOPIC = os.environ.get("KAFKA_TOPIC", "raw.posts")


def main():
    producer = get_producer()
    sources = [HackerNews(limit=50), Reddit(limit=25), BlueskyFirehose(limit=120)]
    total = 0
    for s in sources:
        try:
            posts = s.fetch()
            for post in posts:
                producer.produce(TOPIC, value=post.to_json())
            producer.flush()
            total += len(posts)
            print(f"[{s.name}] produced={len(posts)}")
        except Exception as e:
            print(f"[{s.name}] ERROR: {e}")
    print(f"done. produced total={total} -> topic '{TOPIC}'")


if __name__ == "__main__":
    main()
