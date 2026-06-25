# trendpulse
Real-time social media analytics &amp; trend monitor — distributed streaming pipeline (Kafka, Spark, Django, React, Postgres, Redis) with an agentic AI assistant.

## Deployment (Phase 6)

Production topology (free-tier friendly):

```
 Vercel (React)  ──►  Render web service (Django + Channels, daphne ASGI)
                          │            │
                          ▼            ▼
                   Neon Postgres   Upstash Redis (cache + pub/sub + channels)
                          ▲
        GitHub Action (every 30 min): producers ─► Redpanda Serverless (Kafka)
                                                        └─► consumer ─► Neon
        Spark sentiment job: Databricks (or scheduled) ─► Neon (spark_trends)
```

- **Frontend → Vercel** (`frontend/vercel.json`): set `VITE_API_BASE` to the Render API URL.
- **Backend → Render** (`render.yaml`, `backend/build.sh`): free web service running `daphne` (ASGI, so WebSockets work). Build runs `collectstatic` + `migrate`; static served by WhiteNoise.
- **Postgres → Neon**, **Redis → Upstash** — paste their connection strings into Render env (`DATABASE_URL`, `REDIS_URL`, `DB_SSL=1`).
- **Kafka → Redpanda Serverless** — set `KAFKA_*` env (SASL_SSL). Same code, no changes (env-driven).
- **Ingestion + consumer**: Render background workers/cron are paid, so on the free tier these run as a scheduled **GitHub Action** (`.github/workflows/refresh-data.yml`) using repo secrets.
- **Spark**: runs on Databricks (or any Spark) in production, writing `spark_trends` to Neon.

Secrets are provided via environment variables only; `.env` is gitignored and never committed.

## Scale & Performance

The architecture is designed to scale horizontally; the levers:

- **Sharding / partitioning** — Kafka topic `raw.posts` uses multiple partitions keyed by source, so consumers parallelise; the Postgres `Post` table partitions by source/time for large volumes.
- **Read replicas** — read-heavy endpoints (trending, posts) can target a Postgres read replica (Neon branch / replica) while writes go to the primary; Django DB routing selects per-operation.
- **Caching** — `/api/trending/` is Redis-cached (TTL 60s); measured `X-Cache: MISS` then `HIT`, cutting DB load on the hot path.
- **Load balancing** — the Django service is stateless (all state in Postgres/Redis), so it runs behind a load balancer with N replicas; WebSocket fan-out goes through the Redis channel layer so any instance can serve any client.
- **Backpressure** — Kafka decouples ingestion spikes (e.g. the Bluesky firehose) from processing; consumer groups scale out independently.

**Metrics to capture for the résumé** (instrument via a `/metrics` endpoint or Grafana Cloud): events/sec ingested, p50/p99 API latency, WebSocket fan-out latency, cache hit-rate, total posts processed.
