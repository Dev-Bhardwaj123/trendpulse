#!/usr/bin/env bash
set -euo pipefail
cd /workspaces/trendpulse
echo ">> Applying Phase 4 (Apache Spark: sentiment + trend aggregation) ..."

mkdir -p processing

cat > processing/requirements.txt <<'EOF'
pyspark==3.5.3
vaderSentiment==3.3.2
EOF

cat > processing/spark_job.py <<'EOF'
"""Apache Spark batch job: read raw.posts from Kafka, score sentiment (VADER),
aggregate trending terms with average sentiment, write results to Postgres.

Runs locally (PySpark). Swappable to Databricks at deploy via env vars.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, FloatType)
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "raw.posts")
PG_URL = os.environ.get("SPARK_PG_URL", "jdbc:postgresql://localhost:5432/trendpulse")
PG_PROPS = {"user": "trend", "password": "trend", "driver": "org.postgresql.Driver"}

SCHEMA = StructType([
    StructField("source", StringType()),
    StructField("external_id", StringType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("url", StringType()),
    StructField("author", StringType()),
    StructField("score", IntegerType()),
    StructField("created_at", StringType()),
])

STOP = {"the", "and", "for", "with", "that", "this", "you", "are", "was", "but",
        "not", "have", "has", "from", "your", "all", "out", "get", "got", "now",
        "die", "und", "der", "das", "ich", "ist", "nicht", "ein", "eine", "los",
        "que", "los", "las", "con", "una", "por", "para", "como", "she", "his",
        "her", "they", "them", "who", "what", "when", "why", "how", "can", "will",
        "just", "like", "about", "into", "more", "show", "game"}

_analyzer = SentimentIntensityAnalyzer()


@F.udf(FloatType())
def sentiment(text):
    if not text:
        return 0.0
    return float(_analyzer.polarity_scores(text)["compound"])


def main():
    spark = (SparkSession.builder
             .appName("trendpulse-spark")
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                     "org.postgresql:postgresql:42.7.4")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .load())

    posts = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("p"))
             .select("p.*")
             .filter(F.col("title").isNotNull())
             .dropDuplicates(["source", "external_id"]))

    scored = posts.withColumn("sentiment", sentiment(F.col("title")))

    (scored.select("source", "external_id", "title", "sentiment")
     .write.jdbc(PG_URL, "spark_post_sentiment", mode="overwrite", properties=PG_PROPS))

    words = (scored.select(
                F.explode(F.split(F.lower(F.col("title")), r"[^a-z0-9#+]+")).alias("term"),
                "sentiment")
             .filter(F.length("term") >= 3))
    words = words.filter(~F.col("term").isin(list(STOP)))

    agg = (words.groupBy("term")
           .agg(F.count("*").alias("count"),
                F.round(F.avg("sentiment"), 3).alias("avg_sentiment"))
           .orderBy(F.desc("count"))
           .limit(40))

    agg.write.jdbc(PG_URL, "spark_trends", mode="overwrite", properties=PG_PROPS)

    total = scored.count()
    rows = agg.count()
    avg = scored.agg(F.round(F.avg("sentiment"), 3)).first()[0]
    print(f"[spark] posts_scored={total} trend_terms={rows} overall_avg_sentiment={avg}")
    spark.stop()


if __name__ == "__main__":
    main()
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
    """Spark-produced trends with average sentiment (from spark_trends table)."""
    limit = int(request.query_params.get("limit", 15))
    rows = []
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT term, count, avg_sentiment FROM spark_trends "
                "ORDER BY count DESC LIMIT %s", [limit])
            rows = [{"term": r[0], "count": int(r[1]), "avg_sentiment": float(r[2])}
                    for r in cur.fetchall()]
    except Exception:
        rows = []  # table not created until the Spark job has run
    return Response(rows)
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
EOF

echo ">> Phase 4 files written."
