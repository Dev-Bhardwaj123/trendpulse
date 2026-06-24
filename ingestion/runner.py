from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()
from base import save
from hackernews import HackerNews
from reddit import Reddit


def main():
    sources = [HackerNews(limit=50), Reddit(limit=25)]
    total = 0
    for s in sources:
        try:
            posts = s.fetch()
            inserted = save(posts)
            total += inserted
            print(f"[{s.name}] fetched={len(posts)} upserted={inserted}")
        except Exception as e:
            print(f"[{s.name}] ERROR: {e}")
    print(f"done. total upserted={total}")


if __name__ == "__main__":
    main()
