"""Topic extraction: hashtags + two-word phrases + meaningful keywords.

Filters URLs and a broad multilingual stop-word set so the output reads as
topics rather than noise words.
"""
from __future__ import annotations
import re
from collections import Counter
from datetime import timedelta
from django.utils import timezone
from .models import Post

URL_RE = re.compile(r"https?://\S+|www\.\S+|\b\w+\.(?:com|org|net|io|co)\b")
HASH_RE = re.compile(r"#(\w{2,30})")
WORD_RE = re.compile(r"[A-Za-z][A-Za-z'+\-]{2,}")

STOP = set("""
zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty thirty forty fifty sixty seventy eighty ninety hundred thousand million billion trillion dozen digit digits number numbers word words count
the a an and or of to in for on with is are was were be been being it its as at by from this that
these those there here you your yours we our they them their he she his her him who whom which what
when where why how all any both each few more most other some such only own same so than too very can
will just dont don't cant can't im i'm ive i've youre you're its it's thats that's get got getting
have has had having do does did doing done make makes made making want wants wanted need needs about
into over under after before between out up down off again then once not no nor but if because while
of'' new news now today day days week year years time people world thing things way ways lot bit one
two three first last next back good great best really still even much many about would could should
http https www com org net amp via said says say like likes go goes going gonna wanna let lets
die der das und ist nicht ein eine mit auf den dem von zu sich auch wird war aber als noch nach bei
que los las con una por para como del sus este esta pero mas muy ser son fue han hay este
les des une est pas plus dans sur avec pour qui par mais ont son ses leur nous vous
""".split())


def extract_topics(title: str):
    if not title:
        return []
    hashtags = ["#" + h.lower() for h in HASH_RE.findall(title)]
    clean = URL_RE.sub(" ", title.lower())
    words = [w for w in WORD_RE.findall(clean) if w not in STOP and len(w) >= 4 and not w.isdigit()]
    bigrams = [f"{words[i]} {words[i + 1]}" for i in range(len(words) - 1)]
    return hashtags + bigrams + words


def top_trends(hours: int = 24, limit: int = 20):
    since = timezone.now() - timedelta(hours=hours)
    counter: Counter = Counter()
    weight: Counter = Counter()
    for title in Post.objects.filter(created_at__gte=since).values_list("title", flat=True):
        for term in set(extract_topics(title)):
            counter[term] += 1
            # phrases and hashtags are stronger topic signals than single words
            weight[term] += 3 if (" " in term or term.startswith("#")) else 1
    scored = sorted(counter.keys(), key=lambda t: (weight[t], counter[t]), reverse=True)
    return [{"term": t, "count": counter[t]} for t in scored[:limit]]
