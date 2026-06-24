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
