from django.urls import path
from .consumers import TrendConsumer
websocket_urlpatterns = [path("ws/trends/", TrendConsumer.as_asgi())]
