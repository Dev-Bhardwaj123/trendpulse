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
