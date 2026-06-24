import asyncio
import json
import os
import redis.asyncio as aioredis
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.layers import get_channel_layer

GROUP = "trends"
_bridge_started = False


class TrendConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add(GROUP, self.channel_name)
        await self.accept()
        await self.send(text_data=json.dumps({"type": "connected"}))
        _ensure_bridge()

    async def disconnect(self, code):
        await self.channel_layer.group_discard(GROUP, self.channel_name)

    async def trend_event(self, event):
        await self.send(text_data=json.dumps(event["payload"]))


def _ensure_bridge():
    global _bridge_started
    if _bridge_started:
        return
    _bridge_started = True
    asyncio.create_task(_bridge())


async def _bridge():
    layer = get_channel_layer()
    r = aioredis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))
    pub = r.pubsub()
    await pub.subscribe("newposts")
    async for msg in pub.listen():
        if msg.get("type") != "message":
            continue
        data = msg["data"]
        if isinstance(data, bytes):
            data = data.decode()
        try:
            payload = json.loads(data)
        except Exception:
            payload = {"raw": data}
        await layer.group_send(GROUP, {"type": "trend.event", "payload": payload})
