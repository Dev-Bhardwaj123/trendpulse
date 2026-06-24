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
