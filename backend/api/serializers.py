from rest_framework import serializers
from .models import Post
class PostSerializer(serializers.ModelSerializer):
    class Meta:
        model = Post
        fields = ["id", "source", "title", "url", "author", "score", "created_at"]
