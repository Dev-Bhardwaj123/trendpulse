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
