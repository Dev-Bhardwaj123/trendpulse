#!/usr/bin/env bash
set -euo pipefail
cd /workspaces/trendpulse
echo ">> Applying Phase 5 (agentic AI chatbot - Google Gemini) ..."

cat > backend/requirements.txt <<'EOF'
Django==5.1.4
djangorestframework==3.15.2
django-cors-headers==4.6.0
dj-database-url==2.3.0
psycopg[binary]==3.2.3
gunicorn==23.0.0
python-dotenv==1.0.1
channels==4.1.0
channels-redis==4.2.1
daphne==4.1.2
redis==5.2.1
google-genai==1.9.0
EOF

cat > backend/api/agent.py <<'EOF'
"""Agentic AI assistant powered by Google Gemini with tool-calling.

The model decides which tools to invoke to answer questions over TrendPulse's
own data (trends, Spark sentiment, posts, sources), then synthesises a reply.
"""
import os
from django.db import connection
from django.db.models import Count
from .models import Post
from .trending import top_trends


def get_trending(limit: int = 10) -> list:
    """Get the top trending terms by frequency across all sources in the last 24 hours."""
    return top_trends(limit=limit)


def get_sentiment(limit: int = 10) -> list:
    """Get trending terms with average sentiment from -1 (very negative) to +1 (very positive), computed by the Apache Spark job."""
    rows = []
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT term, count, avg_sentiment FROM spark_trends "
                        "ORDER BY count DESC LIMIT %s", [limit])
            rows = [{"term": r[0], "count": int(r[1]), "avg_sentiment": float(r[2])}
                    for r in cur.fetchall()]
    except Exception:
        pass
    return rows


def search_posts(query: str, limit: int = 8) -> list:
    """Search recent post titles for a keyword. Returns source, title and url for matches."""
    qs = Post.objects.filter(title__icontains=query).order_by("-created_at")[:limit]
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
    model = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
    config = types.GenerateContentConfig(
        tools=[get_trending, get_sentiment, search_posts, get_sources],
        system_instruction=SYSTEM,
    )
    resp = client.models.generate_content(model=model, contents=message, config=config)
    return {"reply": (resp.text or "(no response)"), "ok": True}
EOF

cat > backend/api/views.py <<'EOF'
from django.core.cache import cache
from django.db import connection
from django.db.models import Count
from rest_framework import viewsets, mixins
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import Post
from .serializers import PostSerializer
from .trending import top_trends
from .agent import run_agent


class PostViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    queryset = Post.objects.all()
    serializer_class = PostSerializer
    def get_queryset(self):
        qs = super().get_queryset()
        source = self.request.query_params.get("source")
        return qs.filter(source=source) if source else qs


@api_view(["GET"])
def trending(request):
    hours = int(request.query_params.get("hours", 24))
    limit = int(request.query_params.get("limit", 20))
    key = f"trending:{hours}:{limit}"
    data = cache.get(key)
    cached = data is not None
    if not cached:
        data = top_trends(hours=hours, limit=limit)
        cache.set(key, data, 60)
    resp = Response(data)
    resp["X-Cache"] = "HIT" if cached else "MISS"
    return resp


@api_view(["GET"])
def sources(request):
    data = Post.objects.values("source").annotate(count=Count("id")).order_by("-count")
    return Response(list(data))


@api_view(["GET"])
def sentiment_trends(request):
    limit = int(request.query_params.get("limit", 15))
    rows = []
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT term, count, avg_sentiment FROM spark_trends "
                        "ORDER BY count DESC LIMIT %s", [limit])
            rows = [{"term": r[0], "count": int(r[1]), "avg_sentiment": float(r[2])}
                    for r in cur.fetchall()]
    except Exception:
        rows = []
    return Response(rows)


@api_view(["POST"])
def chat(request):
    msg = (request.data.get("message") or "").strip()
    if not msg:
        return Response({"reply": "Ask me about trending topics or sentiment.", "ok": True})
    try:
        return Response(run_agent(msg))
    except Exception as e:
        return Response({"reply": f"Agent error: {e}", "ok": False})
EOF

cat > backend/api/urls.py <<'EOF'
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views
router = DefaultRouter()
router.register("posts", views.PostViewSet, basename="post")
urlpatterns = [
    path("", include(router.urls)),
    path("trending/", views.trending),
    path("sources/", views.sources),
    path("sentiment/", views.sentiment_trends),
    path("chat/", views.chat),
]
EOF

cat > frontend/src/App.jsx <<'EOF'
import React, { useEffect, useState, useRef } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

const API = import.meta.env.VITE_API_BASE || "/api";

function sentColor(s) {
  if (s > 0.05) return "#4ade80";
  if (s < -0.05) return "#f87171";
  return "#8891b0";
}

function Chat() {
  const [msgs, setMsgs] = useState([
    { role: "bot", text: "Ask me about what's trending or how people feel about a topic." },
  ]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);

  const send = async () => {
    const q = input.trim();
    if (!q || busy) return;
    setMsgs((m) => [...m, { role: "user", text: q }]);
    setInput("");
    setBusy(true);
    try {
      const r = await fetch(`${API}/chat/`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: q }),
      });
      const d = await r.json();
      setMsgs((m) => [...m, { role: "bot", text: d.reply }]);
    } catch (e) {
      setMsgs((m) => [...m, { role: "bot", text: "Error reaching the agent." }]);
    }
    setBusy(false);
  };

  return (
    <section style={{ background: "#141a33", borderRadius: 16, padding: 16, marginBottom: 24 }}>
      <h2 style={{ marginTop: 0 }}>Ask TrendPulse <span style={{ fontSize: 13, opacity: 0.6 }}>(Gemini agent)</span></h2>
      <div style={{ maxHeight: 240, overflowY: "auto", marginBottom: 12 }}>
        {msgs.map((m, i) => (
          <div key={i} style={{ margin: "8px 0", textAlign: m.role === "user" ? "right" : "left" }}>
            <span style={{ display: "inline-block", padding: "8px 12px", borderRadius: 12, maxWidth: "80%",
              background: m.role === "user" ? "#6c8cff" : "#0b1020",
              color: m.role === "user" ? "#06102a" : "#e6e9f2" }}>{m.text}</span>
          </div>
        ))}
        {busy && <div style={{ opacity: 0.6, fontSize: 13 }}>thinking…</div>}
      </div>
      <div style={{ display: "flex", gap: 8 }}>
        <input value={input} onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && send()}
          placeholder="e.g. What's trending and how do people feel about it?"
          style={{ flex: 1, padding: "10px 12px", borderRadius: 10, border: "1px solid #232a47",
            background: "#0b1020", color: "#e6e9f2" }} />
        <button onClick={send} disabled={busy}
          style={{ padding: "10px 18px", borderRadius: 10, border: "none", background: "#6c8cff",
            color: "#06102a", fontWeight: 700, cursor: "pointer" }}>Send</button>
      </div>
    </section>
  );
}

export default function App() {
  const [trends, setTrends] = useState([]);
  const [sentiment, setSentiment] = useState([]);
  const [posts, setPosts] = useState([]);
  const [live, setLive] = useState(false);
  const [flash, setFlash] = useState(false);

  const load = () => {
    fetch(`${API}/trending/?limit=12`).then((r) => r.json()).then(setTrends).catch(() => {});
    fetch(`${API}/sentiment/?limit=12`).then((r) => r.json()).then(setSentiment).catch(() => {});
    fetch(`${API}/posts/`).then((r) => r.json()).then((d) => setPosts(d.results || [])).catch(() => {});
  };

  useEffect(() => {
    load();
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${proto}//${location.host}/ws/trends/`);
    ws.onopen = () => setLive(true);
    ws.onclose = () => setLive(false);
    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.type === "connected") return;
      setFlash(true); setTimeout(() => setFlash(false), 1500); load();
    };
    return () => ws.close();
  }, []);

  const maxC = Math.max(1, ...sentiment.map((d) => d.count));

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: 24 }}>
      <h1 style={{ fontWeight: 800 }}>
        TrendPulse{" "}
        <span style={{ fontSize: 13, padding: "3px 10px", borderRadius: 999,
          background: live ? "#16331f" : "#331616", color: live ? "#4ade80" : "#f87171" }}>
          {live ? "LIVE" : "offline"}
        </span>
        {flash && <span style={{ marginLeft: 10, fontSize: 13, color: "#facc15" }}>new posts received</span>}
      </h1>

      <Chat />

      <section style={{ background: "#141a33", borderRadius: 16, padding: 16, marginBottom: 24 }}>
        <h2 style={{ marginTop: 0 }}>Sentiment by trend <span style={{ fontSize: 13, opacity: 0.6 }}>(Apache Spark + VADER)</span></h2>
        {sentiment.length === 0 && <p style={{ opacity: 0.6 }}>Run the Spark job to populate sentiment.</p>}
        {sentiment.map((d) => (
          <div key={d.term} style={{ display: "flex", alignItems: "center", gap: 12, padding: "4px 0" }}>
            <div style={{ width: 110, textAlign: "right", fontSize: 13 }}>{d.term}</div>
            <div style={{ flex: 1, background: "#0b1020", borderRadius: 6 }}>
              <div style={{ width: `${(d.count / maxC) * 100}%`, background: sentColor(d.avg_sentiment),
                height: 18, borderRadius: 6 }} />
            </div>
            <div style={{ width: 90, fontSize: 12, color: sentColor(d.avg_sentiment) }}>
              {d.avg_sentiment > 0 ? "+" : ""}{d.avg_sentiment}
            </div>
          </div>
        ))}
      </section>

      <section style={{ background: "#141a33", borderRadius: 16, padding: 16, marginBottom: 24 }}>
        <h2 style={{ marginTop: 0 }}>Trending topics (24h)</h2>
        <div style={{ height: 300 }}>
          <ResponsiveContainer>
            <BarChart data={trends} layout="vertical" margin={{ left: 40 }}>
              <XAxis type="number" stroke="#8891b0" allowDecimals={false} />
              <YAxis type="category" dataKey="term" width={120} stroke="#8891b0" />
              <Tooltip />
              <Bar dataKey="count" fill="#6c8cff" radius={[0, 6, 6, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section style={{ background: "#141a33", borderRadius: 16, padding: 16 }}>
        <h2 style={{ marginTop: 0 }}>Latest posts</h2>
        {posts.map((p) => (
          <div key={p.id} style={{ padding: "8px 0", borderBottom: "1px solid #232a47" }}>
            <span style={{ fontSize: 11, color: "#6c8cff", textTransform: "uppercase" }}>{p.source}</span>{" "}
            <a href={p.url} target="_blank" rel="noreferrer" style={{ color: "#e6e9f2" }}>{p.title}</a>{" "}
            <span style={{ opacity: 0.5, fontSize: 12 }}>{p.score}</span>
          </div>
        ))}
      </section>
    </div>
  );
}
EOF

grep -q GEMINI_API_KEY .env || printf 'GEMINI_API_KEY=\nGEMINI_MODEL=gemini-2.0-flash\n' >> .env
grep -q GEMINI_API_KEY .env.example || printf 'GEMINI_API_KEY=\nGEMINI_MODEL=gemini-2.0-flash\n' >> .env.example

echo ">> Phase 5 files written."
