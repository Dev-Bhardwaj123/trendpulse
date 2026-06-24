import React, { useEffect, useState, useRef } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

const API = import.meta.env.VITE_API_BASE || "/api";

function sentColor(s) {
  if (s > 0.05) return "#4ade80";
  if (s < -0.05) return "#f87171";
  return "#8891b0";
}

export default function App() {
  const [trends, setTrends] = useState([]);
  const [sentiment, setSentiment] = useState([]);
  const [posts, setPosts] = useState([]);
  const [live, setLive] = useState(false);
  const [flash, setFlash] = useState(false);
  const wsRef = useRef(null);

  const load = () => {
    fetch(`${API}/trending/?limit=12`).then((r) => r.json()).then(setTrends).catch(() => {});
    fetch(`${API}/sentiment/?limit=12`).then((r) => r.json()).then(setSentiment).catch(() => {});
    fetch(`${API}/posts/`).then((r) => r.json()).then((d) => setPosts(d.results || [])).catch(() => {});
  };

  useEffect(() => {
    load();
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${proto}//${location.host}/ws/trends/`);
    wsRef.current = ws;
    ws.onopen = () => setLive(true);
    ws.onclose = () => setLive(false);
    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.type === "connected") return;
      setFlash(true);
      setTimeout(() => setFlash(false), 1500);
      load();
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
