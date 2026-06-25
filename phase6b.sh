#!/usr/bin/env bash
set -euo pipefail
cd /workspaces/trendpulse
echo ">> Phase 6b: prod WebSocket host fix + Kafka-free direct ingest ..."

cat > frontend/src/App.jsx <<'EOF'
import React, { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

const API = import.meta.env.VITE_API_BASE || "/api";

function wsBase() {
  try {
    const u = new URL(API, window.location.href);
    return (u.protocol === "https:" ? "wss:" : "ws:") + "//" + u.host;
  } catch (e) {
    return (location.protocol === "https:" ? "wss:" : "ws:") + "//" + location.host;
  }
}

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
    let ws;
    try {
      ws = new WebSocket(`${wsBase()}/ws/trends/`);
      ws.onopen = () => setLive(true);
      ws.onclose = () => setLive(false);
      ws.onmessage = (ev) => {
        const msg = JSON.parse(ev.data);
        if (msg.type === "connected") return;
        setFlash(true); setTimeout(() => setFlash(false), 1500); load();
      };
    } catch (e) { /* ws optional */ }
    const poll = setInterval(load, 30000);
    return () => { if (ws) ws.close(); clearInterval(poll); };
  }, []);

  const maxC = Math.max(1, ...sentiment.map((d) => d.count));

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: 24 }}>
      <h1 style={{ fontWeight: 800 }}>
        TrendPulse{" "}
        <span style={{ fontSize: 13, padding: "3px 10px", borderRadius: 999,
          background: live ? "#16331f" : "#331616", color: live ? "#4ade80" : "#f87171" }}>
          {live ? "LIVE" : "polling"}
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

cat > ingestion/ingest_direct.py <<'EOF'
"""Kafka-free ingest: fetch sources and upsert straight into Postgres.

Used by the scheduled GitHub Action to refresh the deployed (Neon) database
without needing a hosted Kafka. The full Kafka pipeline (runner.py + consumer.py)
is the local/streaming path.
"""
from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()
from base import upsert, publish_event
from hackernews import HackerNews
from reddit import Reddit
from bluesky import BlueskyFirehose


def main():
    sources = [HackerNews(limit=50), Reddit(limit=25), BlueskyFirehose(limit=120)]
    total = 0
    for s in sources:
        try:
            posts = s.fetch()
            n = upsert(posts)
            total += n
            print(f"[{s.name}] fetched={len(posts)} upserted={n}")
        except Exception as e:
            print(f"[{s.name}] ERROR: {e}")
    try:
        publish_event({"event": "new_posts", "count": total})
    except Exception:
        pass
    print(f"done. total upserted={total}")


if __name__ == "__main__":
    main()
EOF

cat > .github/workflows/refresh-data.yml <<'EOF'
name: refresh-data
on:
  schedule:
    - cron: "*/30 * * * *"   # every 30 min
  workflow_dispatch:
jobs:
  ingest:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -r ingestion/requirements.txt
      - name: Ingest fresh posts into the deployed database
        working-directory: ingestion
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
          REDIS_URL: ${{ secrets.REDIS_URL }}
        run: python ingest_direct.py
EOF

echo ">> Phase 6b files written."
