#!/usr/bin/env bash
set -euo pipefail
cd /workspaces/trendpulse
echo ">> Applying Phase 6 (deployment configs + production settings + scaling docs) ..."

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
whitenoise==6.8.2
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
ALLOWED_HOSTS = [h.strip() for h in os.environ.get("DJANGO_ALLOWED_HOSTS", "*").split(",") if h.strip()]
CSRF_TRUSTED_ORIGINS = [o.strip() for o in os.environ.get("CSRF_TRUSTED_ORIGINS", "").split(",") if o.strip()]
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
    "whitenoise.middleware.WhiteNoiseMiddleware",
]
ROOT_URLCONF = "config.urls"
TEMPLATES = [{"BACKEND": "django.template.backends.django.DjangoTemplates",
             "DIRS": [], "APP_DIRS": True, "OPTIONS": {"context_processors": []}}]
WSGI_APPLICATION = "config.wsgi.application"
ASGI_APPLICATION = "config.asgi.application"
DATABASES = {"default": dj_database_url.parse(
    os.environ.get("DATABASE_URL", "postgresql://trend:trend@localhost:5432/trendpulse"),
    conn_max_age=600, ssl_require=os.environ.get("DB_SSL", "0") == "1")}
CHANNEL_LAYERS = {"default": {"BACKEND": "channels_redis.core.RedisChannelLayer",
                              "CONFIG": {"hosts": [REDIS_URL]}}}
CACHES = {"default": {"BACKEND": "django.core.cache.backends.redis.RedisCache",
                      "LOCATION": os.environ.get("REDIS_CACHE_URL", REDIS_URL)}}
CORS_ALLOW_ALL_ORIGINS = True
REST_FRAMEWORK = {"DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.PageNumberPagination",
                  "PAGE_SIZE": 50}
STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {"BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage"},
}
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True
EOF

cat > backend/build.sh <<'EOF'
#!/usr/bin/env bash
# Render build step for the Django service.
set -o errexit
pip install -r requirements.txt
python manage.py collectstatic --no-input
python manage.py makemigrations api --no-input
python manage.py migrate --no-input
EOF
chmod +x backend/build.sh

cat > backend/Procfile <<'EOF'
web: daphne -b 0.0.0.0 -p ${PORT:-8000} config.asgi:application
worker: bash -c "cd ../ingestion && python consumer.py"
EOF

cat > render.yaml <<'EOF'
# Render Blueprint. The web service runs on Render's free plan.
# (Background workers / cron are paid on Render; on the free tier the Kafka
# consumer + ingestion run via the scheduled GitHub Action in
# .github/workflows/refresh-data.yml instead.)
services:
  - type: web
    name: trendpulse-api
    runtime: python
    plan: free
    rootDir: backend
    buildCommand: "./build.sh"
    startCommand: "daphne -b 0.0.0.0 -p $PORT config.asgi:application"
    healthCheckPath: /api/sources/
    envVars:
      - key: DJANGO_DEBUG
        value: "0"
      - key: DJANGO_SECRET_KEY
        generateValue: true
      - key: DJANGO_ALLOWED_HOSTS
        value: ".onrender.com"
      - key: DB_SSL
        value: "1"
      - key: DATABASE_URL
        sync: false   # paste Neon connection string
      - key: REDIS_URL
        sync: false   # paste Upstash rediss:// URL
      - key: GEMINI_API_KEY
        sync: false
      - key: GEMINI_MODEL
        value: gemini-2.0-flash-lite
EOF

cat > frontend/vercel.json <<'EOF'
{
  "buildCommand": "npm run build",
  "outputDirectory": "dist",
  "framework": "vite",
  "rewrites": [{ "source": "/(.*)", "destination": "/" }]
}
EOF

cat > frontend/.env.production.example <<'EOF'
# Point the built frontend at the deployed Render API (no trailing slash):
VITE_API_BASE=https://trendpulse-api.onrender.com/api
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
      - name: Produce to Kafka then drain to Postgres
        working-directory: ingestion
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
          REDIS_URL: ${{ secrets.REDIS_URL }}
          KAFKA_BOOTSTRAP: ${{ secrets.KAFKA_BOOTSTRAP }}
          KAFKA_SECURITY_PROTOCOL: SASL_SSL
          KAFKA_SASL_MECHANISM: SCRAM-SHA-256
          KAFKA_SASL_USERNAME: ${{ secrets.KAFKA_SASL_USERNAME }}
          KAFKA_SASL_PASSWORD: ${{ secrets.KAFKA_SASL_PASSWORD }}
        run: |
          python runner.py
          timeout 60 python consumer.py || true
EOF

echo ">> Phase 6 files written."
