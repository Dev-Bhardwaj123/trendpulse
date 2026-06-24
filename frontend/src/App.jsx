import React, { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

const API = import.meta.env.VITE_API_BASE || "http://localhost:8000/api";

export default function App() {
  const [trends, setTrends] = useState([]);
  const [posts, setPosts] = useState([]);
  useEffect(() => {
    const load = () => {
      fetch(`${API}/trending/?limit=12`).then((r) => r.json()).then(setTrends).catch(() => {});
      fetch(`${API}/posts/`).then((r) => r.json()).then((d) => setPosts(d.results || [])).catch(() => {});
    };
    load();
    const t = setInterval(load, 15000);
    return () => clearInterval(t);
  }, []);
  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: 24 }}>
      <h1 style={{ fontWeight: 800 }}>TrendPulse <span style={{ opacity: 0.5, fontSize: 16 }}>live trends</span></h1>
      <section style={{ background: "#141a33", borderRadius: 16, padding: 16, marginBottom: 24 }}>
        <h2 style={{ marginTop: 0 }}>Trending topics (24h)</h2>
        <div style={{ height: 320 }}>
          <ResponsiveContainer>
            <BarChart data={trends} layout="vertical" margin={{ left: 40 }}>
              <XAxis type="number" stroke="#8891b0" />
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
