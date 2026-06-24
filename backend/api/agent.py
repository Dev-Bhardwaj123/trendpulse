"""Agentic AI assistant powered by Google Gemini with tool-calling.

The model decides which tools to invoke to answer questions over TrendPulse's
own data (trends, Spark sentiment, posts, sources), then synthesises a reply.
Gemini function declarations cannot have default parameter values, so the tool
signatures avoid defaults. Automatic function calling is capped to keep the
agent within free-tier limits.
"""
import os
from django.db import connection
from django.db.models import Count
from .models import Post
from .trending import top_trends


def get_trending() -> list:
    """Get the top 12 trending terms by frequency across all sources in the last 24 hours."""
    return top_trends(limit=12)


def get_sentiment() -> list:
    """Get the top 12 trending terms with average sentiment from -1 (very negative) to +1 (very positive), computed by the Apache Spark job."""
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
    """Search recent post titles for a keyword. Returns source, title and url for up to 8 matching posts."""
    qs = Post.objects.filter(title__icontains=query).order_by("-created_at")[:8]
    return [{"source": p.source, "title": p.title, "url": p.url} for p in qs]


def get_sources() -> list:
    """Get each data source and how many posts have been collected from it."""
    return list(Post.objects.values("source").annotate(count=Count("id")).order_by("-count"))


SYSTEM = (
    "You are TrendPulse's analytics assistant. Answer questions about social and "
    "news trends using ONLY the provided tools to fetch real data. Be concise "
    "(2-4 sentences). Mention the actual terms, counts or sentiment values you "
    "found, and which source(s) they came from. If sentiment data is empty, say "
    "the Spark job needs to be run."
)


def run_agent(message: str) -> dict:
    key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not key:
        return {"reply": "Gemini API key not configured. Add GEMINI_API_KEY to .env and restart the server.",
                "ok": False}
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=key)
    model = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash-lite")
    config = types.GenerateContentConfig(
        tools=[get_trending, get_sentiment, search_posts, get_sources],
        system_instruction=SYSTEM,
        automatic_function_calling=types.AutomaticFunctionCallingConfig(maximum_remote_calls=5),
    )
    try:
        resp = client.models.generate_content(model=model, contents=message, config=config)
        return {"reply": (resp.text or "(no response)"), "ok": True}
    except Exception as e:
        m = str(e)
        if "RESOURCE_EXHAUSTED" in m or "429" in m or "quota" in m.lower():
            return {"reply": "Gemini free-tier quota is currently exhausted. Please wait and try again "
                             "(per-minute limits reset quickly; daily limits reset every 24h).",
                    "ok": False}
        return {"reply": f"Agent error: {m[:200]}", "ok": False}
