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
