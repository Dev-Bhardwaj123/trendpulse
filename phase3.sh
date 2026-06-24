#!/usr/bin/env bash
set -euo pipefail
cd /workspaces/trendpulse
echo ">> Applying Phase 3 (Kafka/Redpanda + Bluesky firehose) ..."

cat > docker-compose.yml <<'EOF'
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: trend
      POSTGRES_PASSWORD: trend
      POSTGRES_DB: trendpulse
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:v24.2.7
    container_name: trendpulse-redpanda
    command:
      - redpanda
      - start
      - --mode=dev-container
      - --smp=1
      - --default-log-level=warn
      - --kafka-addr=PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr=PLAINTEXT://localhost:9092
    ports:
      - "9092:9092"
      - "9644:9644"
volumes:
  pgdata:
EOF

cat > ingestion/requirements.txt <<'EOF'
requests==2.32.3
psycopg[binary]==3.2.3
python-dotenv==1.0.1
redis==5.2.1
confluent-kafka==2.6.1
websocket-client==1.8.0
EOF

cat > ingestion/base.py <<'EOF'
from __future__ import annotations
import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import psycopg


@dataclass
class Post:
    source: str
    external_id: str
    title: str
    text: str
    url: str
    author: str
    score: int
    created_at: str  # ISO-8601 string (JSON-transportable through Kafka)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw):
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        return cls(**json.loads(raw))


class Source:
    name = "base"
    def fetch(self):
        raise NotImplementedError


def get_conn():
    dsn = os.environ.get("DATABASE_URL", "postgresql://trend:trend@localhost:5432/trendpulse")
    return psycopg.connect(dsn)


def upsert(posts):
    if not posts:
        return 0
    sql = """
        INSERT INTO api_post
            (source, external_id, title, text, url, author, score, created_at, ingested_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (source, external_id) DO UPDATE SET score = EXCLUDED.score
    """
    now = datetime.now(timezone.utc)
    n = 0
    with get_conn() as conn, conn.cursor() as cur:
        for p in posts:
            cur.execute(sql, (p.source, p.external_id, p.title, p.text,
                              p.url, p.author, p.score, p.created_at, now))
            n += cur.rowcount
    return n


def publish_event(event):
    try:
        import redis
        redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379")).publish(
            "newposts", json.dumps(event))
    except Exception as e:
        print(f"[pubsub] skipped: {e}")
EOF

cat > ingestion/kafka_io.py <<'EOF'
import os


def _conf():
    c = {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")}
    proto = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    c["security.protocol"] = proto
    if proto.startswith("SASL"):
        c["sasl.mechanism"] = os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
        c["sasl.username"] = os.environ.get("KAFKA_SASL_USERNAME", "")
        c["sasl.password"] = os.environ.get("KAFKA_SASL_PASSWORD", "")
    return c


def get_producer():
    from confluent_kafka import Producer
    return Producer(_conf())


def get_consumer(group="trendpulse-consumer"):
    from confluent_kafka import Consumer
    c = _conf()
    c["group.id"] = group
    c["auto.offset.reset"] = "earliest"
    return Consumer(c)
EOF

cat > ingestion/hackernews.py <<'EOF'
from __future__ import annotations
import requests
from datetime import datetime, timezone
from base import Source, Post

API = "https://hacker-news.firebaseio.com/v0"


class HackerNews(Source):
    name = "hackernews"
    def __init__(self, limit=50):
        self.limit = limit
    def fetch(self):
        ids = requests.get(f"{API}/topstories.json", timeout=10).json()[: self.limit]
        out = []
        for i in ids:
            item = requests.get(f"{API}/item/{i}.json", timeout=10).json()
            if not item or item.get("type") != "story":
                continue
            ts = datetime.fromtimestamp(item.get("time", 0), tz=timezone.utc)
            out.append(Post(
                source=self.name, external_id=str(item["id"]),
                title=item.get("title", ""), text=item.get("text", ""),
                url=item.get("url", f"https://news.ycombinator.com/item?id={item['id']}"),
                author=item.get("by", ""), score=int(item.get("score", 0)),
                created_at=ts.isoformat()))
        return out
EOF

cat > ingestion/reddit.py <<'EOF'
from __future__ import annotations
import os
import requests
from datetime import datetime, timezone
from base import Source, Post


class Reddit(Source):
    name = "reddit"
    def __init__(self, subreddits=None, limit=25):
        env_subs = os.environ.get("SUBREDDITS", "technology,programming")
        self.subreddits = subreddits or [s.strip() for s in env_subs.split(",") if s.strip()]
        self.limit = limit
    def fetch(self):
        out = []
        ua = os.environ.get("REDDIT_USER_AGENT", "trendpulse/0.1")
        for sub in self.subreddits:
            url = f"https://www.reddit.com/r/{sub}/hot.json?limit={self.limit}"
            r = requests.get(url, headers={"User-Agent": ua}, timeout=10)
            if r.status_code != 200:
                continue
            for child in r.json().get("data", {}).get("children", []):
                d = child["data"]
                ts = datetime.fromtimestamp(d.get("created_utc", 0), tz=timezone.utc)
                out.append(Post(
                    source=self.name, external_id=d["id"],
                    title=d.get("title", ""), text=d.get("selftext", ""),
                    url="https://www.reddit.com" + d.get("permalink", ""),
                    author=d.get("author", ""), score=int(d.get("score", 0)),
                    created_at=ts.isoformat()))
        return out
EOF

cat > ingestion/bluesky.py <<'EOF'
from __future__ import annotations
import json
from datetime import datetime, timezone
from base import Source, Post

JETSTREAM = ("wss://jetstream2.us-east.bsky.network/subscribe"
             "?wantedCollections=app.bsky.feed.post")


class BlueskyFirehose(Source):
    """Reads the public Bluesky Jetstream firehose (free, no auth)."""
    name = "bluesky"
    def __init__(self, limit=100):
        self.limit = limit
    def fetch(self):
        from websocket import create_connection
        ws = create_connection(JETSTREAM, timeout=30)
        out = []
        try:
            while len(out) < self.limit:
                ev = json.loads(ws.recv())
                if ev.get("kind") != "commit":
                    continue
                commit = ev.get("commit", {})
                if commit.get("operation") != "create":
                    continue
                if commit.get("collection") != "app.bsky.feed.post":
                    continue
                rec = commit.get("record", {})
                text = (rec.get("text") or "").strip()
                if not text:
                    continue
                did = ev.get("did", "")
                rkey = commit.get("rkey", "")
                out.append(Post(
                    source=self.name,
                    external_id=commit.get("cid", f"{did}/{rkey}"),
                    title=text[:300], text=text,
                    url=f"https://bsky.app/profile/{did}/post/{rkey}",
                    author=did, score=0,
                    created_at=rec.get("createdAt") or datetime.now(timezone.utc).isoformat()))
        finally:
            ws.close()
        return out
EOF

cat > ingestion/runner.py <<'EOF'
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
EOF

cat > ingestion/consumer.py <<'EOF'
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
EOF

for v in "KAFKA_BOOTSTRAP=localhost:9092" "KAFKA_TOPIC=raw.posts" "KAFKA_SECURITY_PROTOCOL=PLAINTEXT"; do
  k="${v%%=*}"
  grep -q "$k" .env || printf '%s\n' "$v" >> .env
  grep -q "$k" .env.example || printf '%s\n' "$v" >> .env.example
done

echo ">> Phase 3 files written."
