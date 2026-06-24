from __future__ import annotations
import os
from dataclasses import dataclass
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
    created_at: datetime

    def as_row(self):
        return (self.source, self.external_id, self.title, self.text,
                self.url, self.author, self.score, self.created_at)


class Source:
    name = "base"
    def fetch(self):
        raise NotImplementedError


def get_conn():
    dsn = os.environ.get("DATABASE_URL", "postgresql://trend:trend@localhost:5432/trendpulse")
    return psycopg.connect(dsn)


def save(posts):
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
            cur.execute(sql, (*p.as_row(), now))
            n += cur.rowcount
    return n
