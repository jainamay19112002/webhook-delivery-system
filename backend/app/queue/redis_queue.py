import json
import logging
from typing import Dict, Any, List, Optional
import redis.asyncio as aioredis
from app.config import settings

logger = logging.getLogger("redis_queue")

class RedisStreamQueue:
    def __init__(self, redis_url: str = settings.REDIS_URL):
        self.redis_url = redis_url
        self.client: Optional[aioredis.Redis] = None
        self.is_fake = False

    async def connect(self):
        try:
            client = aioredis.from_url(self.redis_url, decode_responses=True)
            await client.ping()
            self.client = client
            logger.info("Connected to live Redis server.")
        except Exception as e:
            if settings.USE_FAKEREDIS_IF_OFFLINE:
                logger.warning(f"Could not connect to Redis at {self.redis_url} ({e}). Falling back to fakeredis.")
                import fakeredis.aioredis
                self.client = fakeredis.aioredis.FakeRedis(decode_responses=True)
                self.is_fake = True
            else:
                raise e

        # Ensure Stream Consumer Group exists
        try:
            await self.client.xgroup_create(
                name=settings.STREAM_KEY,
                groupname=settings.CONSUMER_GROUP,
                id="0",
                mkstream=True
            )
        except Exception:
            # Group might already exist
            pass

    async def close(self):
        if self.client:
            await self.client.close()

    async def publish_event(self, event_id: str, event_type: str, subscriber_ids: List[str], payload: Dict[str, Any]) -> str:
        """
        Publishes event to Redis Stream 'webhook:events'.
        """
        if not self.client:
            await self.connect()

        data = {
            "event_id": event_id,
            "event_type": event_type,
            "subscriber_ids": json.dumps(subscriber_ids),
            "payload": json.dumps(payload)
        }

        entry_id = await self.client.xadd(settings.STREAM_KEY, data)
        return entry_id

    async def read_events(self, consumer_name: str, count: int = 10, block_ms: int = 2000) -> List[tuple[str, Dict[str, Any]]]:
        """
        Reads pending events from Consumer Group via XREADGROUP.
        """
        if not self.client:
            await self.connect()

        try:
            results = await self.client.xreadgroup(
                groupname=settings.CONSUMER_GROUP,
                consumername=consumer_name,
                streams={settings.STREAM_KEY: ">"},
                count=count,
                block=block_ms
            )
            
            events = []
            if results:
                for stream_name, stream_entries in results:
                    for entry_id, entry_data in stream_entries:
                        events.append((entry_id, entry_data))
            return events
        except Exception as e:
            logger.error(f"Error reading from Redis Stream: {e}")
            return []

    async def acknowledge_event(self, entry_id: str):
        """
        XACK acknowledge that event delivery is finalized or passed to DLQ.
        """
        if not self.client:
            return
        await self.client.xack(settings.STREAM_KEY, settings.CONSUMER_GROUP, entry_id)

    async def push_to_dlq(self, event_id: str, subscriber_id: str, reason: str, attempts: int):
        """
        Pushes un-deliverable event to Dead Letter Queue (DLQ).
        """
        if not self.client:
            await self.connect()

        dlq_data = {
            "event_id": event_id,
            "subscriber_id": subscriber_id,
            "reason": reason,
            "attempts": str(attempts)
        }
        await self.client.xadd(settings.DLQ_STREAM_KEY, dlq_data)

queue_client = RedisStreamQueue()
