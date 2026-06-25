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
