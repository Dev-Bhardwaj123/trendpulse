#!/usr/bin/env bash
set -euo pipefail
cd /workspaces/trendpulse
echo ">> Applying Phase 2 (Redis cache + WebSockets + pub/sub) ..."

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
EOF

cat > backend/config/settings.py <<'EOF'
import os
from pathlib import Path
import dj_database_url
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR.parent / ".env")

SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "dev-insecure")
DEBUG = os.environ.get("DJANGO_DEBUG", "1") == "1"
ALLOWED_HOSTS = os.environ.get("DJANGO_ALLOWED_HOSTS", "*").split(",")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

INSTALLED_APPS = [
    "daphne",
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django.contrib.staticfiles",
    "channels",
    "rest_framework",
    "corsheaders",
    "api",
]
MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.common.CommonMiddleware",
]
ROOT_URLCONF = "config.urls"
TEMPLATES = [{"BACKEND": "django.template.backends.django.DjangoTemplates",
             "DIRS": [], "APP_DIRS": True, "OPTIONS": {"context_processors": []}}]
WSGI_APPLICATION = "config.wsgi.application"
ASGI_APPLICATION = "config.asgi.application"
DATABASES = {"default": dj_database_url.parse(
    os.environ.get("DATABASE_URL", "postgresql://trend:trend@localhost:5432/trendpulse"),
    conn_max_age=600)}
CHANNEL_LAYERS = {"default": {"BACKEND": "channels_redis.core.RedisChannelLayer",
                              "CONFIG": {"hosts": [REDIS_URL]}}}
CACHES = {"default": {"BACKEND": "django.core.cache.backends.redis.RedisCache",
                      "LOCATION": REDIS_URL + "/1"}}
CORS_ALLOW_ALL_ORIGINS = True
REST_FRAMEWORK = {"DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.PageNumberPagination",
                  "PAGE_SIZE": 50}
STATIC_URL = "static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True
EOF

cat > backend/config/asgi.py <<'EOF'
import os
from django.core.asgi import get_asgi_application
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django_asgi_app = get_asgi_application()

from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
from api.routing import websocket_urlpatterns

application = ProtocolTypeRouter({
    "http": django_asgi_app,
    "websocket": AuthMiddlewareStack(URLRouter(websocket_urlpatterns)),
})
EOF

cat > backend/api/routing.py <<'EOF'
from django.urls import path
from .consumers import TrendConsumer
websocket_urlpatterns = [path("ws/trends/", TrendConsumer.as_asgi())]
EOF

cat > backend/api/consumers.py <<'EOF'
import asyncio
import json
import os
import redis.asyncio as aioredis
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.layers import get_channel_layer

GROUP = "trends"
_bridge_started = False


class TrendConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add(GROUP, self.channel_name)
        await self.accept()
        await self.send(text_data=json.dumps({"type": "connected"}))
        _ensure_bridge()

    async def disconnect(self, code):
        await self.channel_layer.group_discard(GROUP, self.channel_name)

    async def trend_event(self, event):
        await self.send(text_data=json.dumps(event["payload"]))


def _ensure_bridge():
    global _bridge_started
    if _bridge_started:
        return
    _bridge_started = True
    asyncio.create_task(_bridge())


async def _bridge():
    layer = get_channel_layer()
    r = aioredis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))
    pub = r.pubsub()
    await pub.subscribe("newposts")
    async for msg in pub.listen():
        if msg.get("type") != "message":
            continue
        data = msg["data"]
        if isinstance(data, bytes):
            data = data.decode()
        try:
            payload = json.loads(data)
        except Exception:
            payload = {"raw": data}
        await layer.group_send(GROUP, {"type": "trend.event", "payload": payload})
EOF

cat > backend/api/views.py <<'EOF'
from django.core.cache import cache
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
EOF

cat > ingestion/requirements.txt <<'EOF'
requests==2.32.3
psycopg[binary]==3.2.3
python-dotenv==1.0.1
redis==5.2.1
EOF

cat > ingestion/base.py <<'EOF'
from __future__ import annotations
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
import psycopg


@dataclass
class Post:
    source: str
    external_id: str
    title: str
    text: str
    url: str
    author: str
    score: int
    created_at: datetime

    def as_row(self):
        return (self.source, self.external_id, self.title, self.text,
                self.url, self.author, self.score, self.created_at)


class Source:
    name = "base"
    def fetch(self):
        raise NotImplementedError


def get_conn():
    dsn = os.environ.get("DATABASE_URL", "postgresql://trend:trend@localhost:5432/trendpulse")
    return psycopg.connect(dsn)


def _publish(event):
    try:
        import redis
        r = redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))
        r.publish("newposts", json.dumps(event))
    except Exception as e:
        print(f"[pubsub] skipped: {e}")


def save(posts):
    if not posts:
        return 0
    sql = """
        INSERT INTO api_post
            (source, external_id, title, text, url, author, score, created_at, ingested_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (source, external_id) DO UPDATE SET score = EXCLUDED.score
    """
    now = datetime.now(timezone.utc)
    n = 0
    sources = set()
    with get_conn() as conn, conn.cursor() as cur:
        for p in posts:
            cur.execute(sql, (*p.as_row(), now))
            n += cur.rowcount
            sources.add(p.source)
    _publish({"event": "new_posts", "count": n, "sources": sorted(sources)})
    return n
EOF

cat > frontend/vite.config.js <<'EOF'
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    host: true,
    proxy: {
      "/api": "http://localhost:8000",
      "/ws": { target: "ws://localhost:8000", ws: true },
    },
  },
});
EOF

cat > frontend/src/App.jsx <<'EOF'
import React, { useEffect, useState, useRef } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

const API = import.meta.env.VITE_API_BASE || "/api";

export default function App() {
  const [trends, setTrends] = useState([]);
  const [posts, setPosts] = useState([]);
  const [live, setLive] = useState(false);
  const [flash, setFlash] = useState(false);
  const wsRef = useRef(null);

  const load = () => {
    fetch(`${API}/trending/?limit=12`).then((r) => r.json()).then(setTrends).catch(() => {});
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
      <section style={{ background: "#141a33", borderRadius: 16, padding: 16, marginBottom: 24,
        transition: "outline .3s", outline: flash ? "2px solid #facc15" : "2px solid transparent" }}>
        <h2 style={{ marginTop: 0 }}>Trending topics (24h)</h2>
        <div style={{ height: 320 }}>
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

grep -q REDIS_URL .env || printf 'REDIS_URL=redis://localhost:6379\n' >> .env
grep -q REDIS_URL .env.example || printf 'REDIS_URL=redis://localhost:6379\n' >> .env.example

echo ">> Phase 2 files written."
