#!/usr/bin/env bash
set -euo pipefail
cd /workspaces/trendpulse
echo ">> Phase 7: real topic extraction + agent data fallback + Neon sentiment ..."

cat > backend/api/trending.py <<'EOF'
"""Topic extraction: hashtags + two-word phrases + meaningful keywords.

Filters URLs and a broad multilingual stop-word set so the output reads as
topics rather than noise words.
"""
from __future__ import annotations
import re
from collections import Counter
from datetime import timedelta
from django.utils import timezone
from .models import Post

URL_RE = re.compile(r"https?://\S+|www\.\S+|\b\w+\.(?:com|org|net|io|co)\b")
HASH_RE = re.compile(r"#(\w{2,30})")
WORD_RE = re.compile(r"[A-Za-z][A-Za-z'+\-]{2,}")

STOP = set("""
the a an and or of to in for on with is are was were be been being it its as at by from this that
these those there here you your yours we our they them their he she his her him who whom which what
when where why how all any both each few more most other some such only own same so than too very can
will just dont don't cant can't im i'm ive i've youre you're its it's thats that's get got getting
have has had having do does did doing done make makes made making want wants wanted need needs about
into over under after before between out up down off again then once not no nor but if because while
of'' new news now today day days week year years time people world thing things way ways lot bit one
two three first last next back good great best really still even much many about would could should
http https www com org net amp via said says say like likes go goes going gonna wanna let lets
die der das und ist nicht ein eine mit auf den dem von zu sich auch wird war aber als noch nach bei
que los las con una por para como del sus este esta pero mas muy ser son fue han hay este
les des une est pas plus dans sur avec pour qui par mais ont son ses leur nous vous
""".split())


def extract_topics(title: str):
    if not title:
        return []
    hashtags = ["#" + h.lower() for h in HASH_RE.findall(title)]
    clean = URL_RE.sub(" ", title.lower())
    words = [w for w in WORD_RE.findall(clean) if w not in STOP and len(w) >= 4 and not w.isdigit()]
    bigrams = [f"{words[i]} {words[i + 1]}" for i in range(len(words) - 1)]
    return hashtags + bigrams + words


def top_trends(hours: int = 24, limit: int = 20):
    since = timezone.now() - timedelta(hours=hours)
    counter: Counter = Counter()
    weight: Counter = Counter()
    for title in Post.objects.filter(created_at__gte=since).values_list("title", flat=True):
        for term in set(extract_topics(title)):
            counter[term] += 1
            # phrases and hashtags are stronger topic signals than single words
            weight[term] += 3 if (" " in term or term.startswith("#")) else 1
    scored = sorted(counter.keys(), key=lambda t: (weight[t], counter[t]), reverse=True)
    return [{"term": t, "count": counter[t]} for t in scored[:limit]]
EOF

cat > backend/api/agent.py <<'EOF'
"""Agentic AI assistant (Google Gemini, tool-calling) with a data fallback.

If Gemini is unavailable (e.g. free-tier quota), the agent still answers using a
deterministic summary built from the same tools, so the chat always returns real
data instead of an error.
"""
import os
from django.db import connection
from django.db.models import Count
from .models import Post
from .trending import top_trends


def get_trending() -> list:
    """Get the top 12 trending topics across all sources in the last 24 hours."""
    return top_trends(limit=12)


def get_sentiment() -> list:
    """Get trending topics with average sentiment from -1 (very negative) to +1 (very positive)."""
    rows = []
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT term, count, avg_sentiment FROM spark_trends "
                        "ORDER BY count DESC LIMIT 12")
            rows = [{"term": r[0], "count": int(r[1]), "avg_sentiment": float(r[2])}
                    for r in cur.fetchall()]
    except Exception:
        pass
    return rows


def search_posts(query: str) -> list:
    """Search recent post titles for a keyword. Returns source, title and url for up to 8 matches."""
    qs = Post.objects.filter(title__icontains=query).order_by("-created_at")[:8]
    return [{"source": p.source, "title": p.title, "url": p.url} for p in qs]


def get_sources() -> list:
    """Get each data source and how many posts have been collected from it."""
    return list(Post.objects.values("source").annotate(count=Count("id")).order_by("-count"))


SYSTEM = (
    "You are TrendPulse's analytics assistant. Answer questions about social and "
    "news trends using ONLY the provided tools to fetch real data. Be concise "
    "(2-4 sentences). Mention the actual topics, counts or sentiment values you found."
)


def _fallback(message: str) -> str:
    trends = get_trending()
    sent = get_sentiment()
    bits = []
    if trends:
        bits.append("Top trending topics right now: "
                    + ", ".join(t["term"] for t in trends[:6]) + ".")
    if sent:
        pos = max(sent, key=lambda x: x["avg_sentiment"])
        neg = min(sent, key=lambda x: x["avg_sentiment"])
        bits.append(f"Most positive: \"{pos['term']}\" ({pos['avg_sentiment']:+.2f}); "
                    f"most negative: \"{neg['term']}\" ({neg['avg_sentiment']:+.2f}).")
    if not bits:
        return "No data yet — run an ingest to populate trends, then ask again."
    return "(AI model is rate-limited, so here's a direct summary of the live data.) " + " ".join(bits)


def run_agent(message: str) -> dict:
    key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not key:
        return {"reply": _fallback(message), "ok": True}
    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=key)
        model = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash-lite")
        config = types.GenerateContentConfig(
            tools=[get_trending, get_sentiment, search_posts, get_sources],
            system_instruction=SYSTEM,
            automatic_function_calling=types.AutomaticFunctionCallingConfig(maximum_remote_calls=5),
        )
        resp = client.models.generate_content(model=model, contents=message, config=config)
        text = (resp.text or "").strip()
        return {"reply": text or _fallback(message), "ok": True}
    except Exception as e:
        m = str(e)
        if "RESOURCE_EXHAUSTED" in m or "429" in m or "quota" in m.lower():
            return {"reply": _fallback(message), "ok": True}
        return {"reply": _fallback(message), "ok": True}
EOF

cat > ingestion/ingest_direct.py <<'EOF'
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
EOF

echo ">> Phase 7 files written."
