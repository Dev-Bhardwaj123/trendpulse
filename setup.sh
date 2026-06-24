#!/usr/bin/env bash
set -euo pipefail
ROOT="."
echo ">> Generating TrendPulse Phase 1 project ..."
mkdir -p "$ROOT"/ingestion "$ROOT"/backend/config "$ROOT"/backend/api "$ROOT"/frontend/src "$ROOT"/.github/workflows
cd "$ROOT"

cat > .gitignore <<'EOF'
__pycache__/
*.py[cod]
.venv/
venv/
*.sqlite3
.env
node_modules/
dist/
.DS_Store
*.log
EOF

cat > .env.example <<'EOF'
DATABASE_URL=postgresql://trend:trend@localhost:5432/trendpulse
DJANGO_SECRET_KEY=dev-insecure-change-me
DJANGO_DEBUG=1
DJANGO_ALLOWED_HOSTS=*
REDDIT_USER_AGENT=trendpulse/0.1 by yourusername
SUBREDDITS=technology,programming,artificial,MachineLearning
EOF

cat > docker-compose.yml <<'EOF'
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: trend
      POSTGRES_PASSWORD: trend
      POSTGRES_DB: trendpulse
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
volumes:
  pgdata:
EOF

cat > ingestion/requirements.txt <<'EOF'
requests==2.32.3
psycopg[binary]==3.2.3
python-dotenv==1.0.1
EOF

cat > ingestion/base.py <<'EOF'
from __future__ import annotations
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
    with get_conn() as conn, conn.cursor() as cur:
        for p in posts:
            cur.execute(sql, (*p.as_row(), now))
            n += cur.rowcount
    return n
EOF

cat > ingestion/hackernews.py <<'EOF'
from __future__ import annotations
import requests
from datetime import datetime, timezone
from base import Source, Post

API = "https://hacker-news.firebaseio.com/v0"


class HackerNews(Source):
    name = "hackernews"
    def __init__(self, limit=50):
        self.limit = limit
    def fetch(self):
        ids = requests.get(f"{API}/topstories.json", timeout=10).json()[: self.limit]
        out = []
        for i in ids:
            item = requests.get(f"{API}/item/{i}.json", timeout=10).json()
            if not item or item.get("type") != "story":
                continue
            out.append(Post(
                source=self.name, external_id=str(item["id"]),
                title=item.get("title", ""), text=item.get("text", ""),
                url=item.get("url", f"https://news.ycombinator.com/item?id={item['id']}"),
                author=item.get("by", ""), score=int(item.get("score", 0)),
                created_at=datetime.fromtimestamp(item.get("time", 0), tz=timezone.utc)))
        return out
EOF

cat > ingestion/reddit.py <<'EOF'
from __future__ import annotations
import os
import requests
from datetime import datetime, timezone
from base import Source, Post


class Reddit(Source):
    name = "reddit"
    def __init__(self, subreddits=None, limit=25):
        env_subs = os.environ.get("SUBREDDITS", "technology,programming")
        self.subreddits = subreddits or [s.strip() for s in env_subs.split(",") if s.strip()]
        self.limit = limit
    def fetch(self):
        out = []
        ua = os.environ.get("REDDIT_USER_AGENT", "trendpulse/0.1")
        for sub in self.subreddits:
            url = f"https://www.reddit.com/r/{sub}/hot.json?limit={self.limit}"
            r = requests.get(url, headers={"User-Agent": ua}, timeout=10)
            if r.status_code != 200:
                continue
            for child in r.json().get("data", {}).get("children", []):
                d = child["data"]
                out.append(Post(
                    source=self.name, external_id=d["id"],
                    title=d.get("title", ""), text=d.get("selftext", ""),
                    url="https://www.reddit.com" + d.get("permalink", ""),
                    author=d.get("author", ""), score=int(d.get("score", 0)),
                    created_at=datetime.fromtimestamp(d.get("created_utc", 0), tz=timezone.utc)))
        return out
EOF

cat > ingestion/runner.py <<'EOF'
from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()
from base import save
from hackernews import HackerNews
from reddit import Reddit


def main():
    sources = [HackerNews(limit=50), Reddit(limit=25)]
    total = 0
    for s in sources:
        try:
            posts = s.fetch()
            inserted = save(posts)
            total += inserted
            print(f"[{s.name}] fetched={len(posts)} upserted={inserted}")
        except Exception as e:
            print(f"[{s.name}] ERROR: {e}")
    print(f"done. total upserted={total}")


if __name__ == "__main__":
    main()
EOF

cat > backend/requirements.txt <<'EOF'
Django==5.1.4
djangorestframework==3.15.2
django-cors-headers==4.6.0
dj-database-url==2.3.0
psycopg[binary]==3.2.3
gunicorn==23.0.0
python-dotenv==1.0.1
EOF

cat > backend/manage.py <<'EOF'
#!/usr/bin/env python
import os
import sys
if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    from django.core.management import execute_from_command_line
    execute_from_command_line(sys.argv)
EOF

touch backend/config/__init__.py

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

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django.contrib.staticfiles",
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
DATABASES = {"default": dj_database_url.parse(
    os.environ.get("DATABASE_URL", "postgresql://trend:trend@localhost:5432/trendpulse"),
    conn_max_age=600)}
CORS_ALLOW_ALL_ORIGINS = True
REST_FRAMEWORK = {"DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.PageNumberPagination",
                  "PAGE_SIZE": 50}
STATIC_URL = "static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True
EOF

cat > backend/config/urls.py <<'EOF'
from django.urls import path, include
urlpatterns = [path("api/", include("api.urls"))]
EOF

cat > backend/config/wsgi.py <<'EOF'
import os
from django.core.wsgi import get_wsgi_application
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
application = get_wsgi_application()
EOF

touch backend/api/__init__.py

cat > backend/api/apps.py <<'EOF'
from django.apps import AppConfig
class ApiConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "api"
EOF

cat > backend/api/models.py <<'EOF'
from django.db import models
class Post(models.Model):
    source = models.CharField(max_length=64, db_index=True)
    external_id = models.CharField(max_length=128)
    title = models.TextField()
    text = models.TextField(blank=True)
    url = models.URLField(max_length=1000, blank=True)
    author = models.CharField(max_length=255, blank=True)
    score = models.IntegerField(default=0)
    created_at = models.DateTimeField(db_index=True)
    ingested_at = models.DateTimeField(auto_now_add=True)
    class Meta:
        unique_together = ("source", "external_id")
        ordering = ["-created_at"]
    def __str__(self):
        return f"[{self.source}] {self.title[:60]}"
EOF

cat > backend/api/serializers.py <<'EOF'
from rest_framework import serializers
from .models import Post
class PostSerializer(serializers.ModelSerializer):
    class Meta:
        model = Post
        fields = ["id", "source", "title", "url", "author", "score", "created_at"]
EOF

cat > backend/api/trending.py <<'EOF'
from __future__ import annotations
import re
from collections import Counter
from datetime import timedelta
from django.utils import timezone
from .models import Post

STOP = set("the a an and or of to in for on with is are was be this that it as at by from i you we they he she them his her our your my me do does did how what when why who which will just like get got new use using can not no yes if then than so but about into over more most".split())
WORD = re.compile(r"[A-Za-z][A-Za-z0-9+#.\-]{2,}")

def top_trends(hours=24, limit=20):
    since = timezone.now() - timedelta(hours=hours)
    counter = Counter()
    for title in Post.objects.filter(created_at__gte=since).values_list("title", flat=True):
        for w in WORD.findall(title.lower()):
            if w not in STOP:
                counter[w] += 1
    return [{"term": t, "count": c} for t, c in counter.most_common(limit)]
EOF

cat > backend/api/views.py <<'EOF'
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
    return Response(top_trends(hours=hours, limit=limit))


@api_view(["GET"])
def sources(request):
    data = Post.objects.values("source").annotate(count=Count("id")).order_by("-count")
    return Response(list(data))
EOF

cat > backend/api/urls.py <<'EOF'
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views
router = DefaultRouter()
router.register("posts", views.PostViewSet, basename="post")
urlpatterns = [path("", include(router.urls)),
               path("trending/", views.trending),
               path("sources/", views.sources)]
EOF

cat > backend/api/tests.py <<'EOF'
from django.test import TestCase
from django.utils import timezone
from .models import Post
from .trending import top_trends


class TrendingTests(TestCase):
    def test_top_trends_counts_keywords(self):
        for i in range(3):
            Post.objects.create(source="test", external_id=str(i),
                                title="Python and AI agents", created_at=timezone.now())
        terms = {d["term"]: d["count"] for d in top_trends()}
        self.assertEqual(terms.get("python"), 3)
        self.assertEqual(terms.get("agents"), 3)

    def test_api_posts_endpoint(self):
        Post.objects.create(source="test", external_id="x", title="hi", created_at=timezone.now())
        resp = self.client.get("/api/posts/")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["count"], 1)
EOF

cat > backend/Dockerfile <<'EOF'
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN python manage.py collectstatic --noinput || true
CMD gunicorn config.wsgi --bind 0.0.0.0:${PORT:-8000}
EOF

cat > frontend/package.json <<'EOF'
{
  "name": "trendpulse-frontend",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {"dev": "vite", "build": "vite build", "preview": "vite preview"},
  "dependencies": {"react": "^18.3.1", "react-dom": "^18.3.1", "recharts": "^2.13.3"},
  "devDependencies": {"@vitejs/plugin-react": "^4.3.4", "vite": "^6.0.3"}
}
EOF

cat > frontend/vite.config.js <<'EOF'
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
export default defineConfig({ plugins: [react()], server: { port: 5173, host: true } });
EOF

cat > frontend/index.html <<'EOF'
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>TrendPulse</title>
  </head>
  <body style="margin:0;font-family:system-ui,sans-serif;background:#0b1020;color:#e6e9f2">
    <div id="root"></div>
    <script type="module" src="/src/main.jsx"></script>
  </body>
</html>
EOF

cat > frontend/.env.example <<'EOF'
VITE_API_BASE=http://localhost:8000/api
EOF

cat > frontend/src/main.jsx <<'EOF'
import React from "react";
import { createRoot } from "react-dom/client";
import App from "./App.jsx";
createRoot(document.getElementById("root")).render(<App />);
EOF

cat > frontend/src/App.jsx <<'EOF'
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
EOF

cat > .github/workflows/backend-ci.yml <<'EOF'
name: backend-ci
on:
  push:
    branches: [main]
  pull_request:
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_USER: trend
          POSTGRES_PASSWORD: trend
          POSTGRES_DB: trendpulse
        ports: ["5432:5432"]
        options: >-
          --health-cmd pg_isready --health-interval 10s
          --health-timeout 5s --health-retries 5
    env:
      DATABASE_URL: postgresql://trend:trend@localhost:5432/trendpulse
      DJANGO_SECRET_KEY: ci-secret
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -r backend/requirements.txt ruff
      - run: ruff check backend ingestion || true
      - working-directory: backend
        run: |
          python manage.py migrate
          python manage.py test
EOF

echo ">> Project files written."
