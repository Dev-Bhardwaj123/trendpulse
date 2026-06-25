"""Kafka-free ingest for the deployed DB: upsert posts AND compute VADER
sentiment per topic, writing spark_trends. Used by the scheduled GitHub Action.
"""
from __future__ import annotations
import re
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()
from base import upsert, publish_event, get_conn
from hackernews import HackerNews
from reddit import Reddit
from bluesky import BlueskyFirehose
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

URL_RE = re.compile(r"https?://\S+|www\.\S+|\b\w+\.(?:com|org|net|io|co)\b")
HASH_RE = re.compile(r"#(\w{2,30})")
WORD_RE = re.compile(r"[A-Za-z][A-Za-z'+\-]{2,}")
STOP = set("""
the a an and or of to in for on with is are was were be been being it its as at by from this that
these those there here you your we our they them their he she his her who which what when where why
how all any both each more most other some such only own same so than too very can will just dont
have has had do does did make made want need about into over after before out up down off again then
new news now today day days week year years time people world thing things way one two first last next
back good great best really still even much many would could should http https www com org net amp via
die der das und ist nicht ein eine mit auf den dem von zu sich auch wird war aber als noch nach bei
que los las con una por para como del este esta pero mas muy ser son fue les des une est pas plus dans
sur avec pour qui par mais ont son ses leur nous vous
""".split())


def topics(title):
    if not title:
        return []
    hashtags = ["#" + h.lower() for h in HASH_RE.findall(title)]
    words = [w for w in WORD_RE.findall(URL_RE.sub(" ", title.lower()))
             if w not in STOP and len(w) >= 4 and not w.isdigit()]
    bigrams = [f"{words[i]} {words[i + 1]}" for i in range(len(words) - 1)]
    return hashtags + bigrams + words


def populate_spark_trends():
    an = SentimentIntensityAnalyzer()
    agg = defaultdict(lambda: [0, 0.0])     # term -> [count, sentiment_sum]
    wt = defaultdict(int)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT title FROM api_post WHERE created_at > now() - interval '7 days'")
            for (title,) in cur.fetchall():
                s = an.polarity_scores(title or "")["compound"]
                for t in set(topics(title)):
                    agg[t][0] += 1
                    agg[t][1] += s
                    wt[t] += 3 if (" " in t or t.startswith("#")) else 1
        ranked = sorted(agg.items(), key=lambda kv: (wt[kv[0]], kv[1][0]), reverse=True)
        rows = [(t, c, round(sv / c, 3)) for t, (c, sv) in ranked if c >= 2][:40]
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS spark_trends")
            cur.execute("CREATE TABLE spark_trends (term text, count integer, avg_sentiment double precision)")
            cur.executemany("INSERT INTO spark_trends (term, count, avg_sentiment) VALUES (%s,%s,%s)", rows)
        conn.commit()
    print(f"[sentiment] spark_trends rows={len(rows)}")


def main():
    total = 0
    for s in [HackerNews(limit=50), Reddit(limit=25), BlueskyFirehose(limit=120)]:
        try:
            posts = s.fetch()
            n = upsert(posts)
            total += n
            print(f"[{s.name}] fetched={len(posts)} upserted={n}")
        except Exception as e:
            print(f"[{s.name}] ERROR: {e}")
    try:
        populate_spark_trends()
    except Exception as e:
        print(f"[sentiment] ERROR: {e}")
    try:
        publish_event({"event": "new_posts", "count": total})
    except Exception:
        pass
    print(f"done. total upserted={total}")


if __name__ == "__main__":
    main()
