"""Kafka-free ingest: fetch sources and upsert straight into Postgres.

Used by the scheduled GitHub Action to refresh the deployed (Neon) database
without needing a hosted Kafka. The full Kafka pipeline (runner.py + consumer.py)
is the local/streaming path.
"""
from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()
from base import upsert, publish_event
from hackernews import HackerNews
from reddit import Reddit
from bluesky import BlueskyFirehose


def main():
    sources = [HackerNews(limit=50), Reddit(limit=25), BlueskyFirehose(limit=120)]
    total = 0
    for s in sources:
        try:
            posts = s.fetch()
            n = upsert(posts)
            total += n
            print(f"[{s.name}] fetched={len(posts)} upserted={n}")
        except Exception as e:
            print(f"[{s.name}] ERROR: {e}")
    try:
        publish_event({"event": "new_posts", "count": total})
    except Exception:
        pass
    print(f"done. total upserted={total}")


if __name__ == "__main__":
    main()
