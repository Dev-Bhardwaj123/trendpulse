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
