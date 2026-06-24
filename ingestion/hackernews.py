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
