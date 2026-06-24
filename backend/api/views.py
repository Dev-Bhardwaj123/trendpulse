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
