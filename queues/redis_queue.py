"""
Redis queue infrastructure for async agent communication.
Implements message queue with retry logic, exponential backoff, and DLQ.
"""
import asyncio
import json
import logging
from typing import Optional, Callable, Any, Dict
from datetime import datetime, timedelta
import redis.asyncio as aioredis
from redis.asyncio import Redis
from pydantic import BaseModel

from config import settings


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

logger = logging.getLogger(__name__)


class QueueConfig:
    """Queue naming and configuration constants."""
    
    # Queue names
    PLANNER_QUEUE = "queue:planner"
    RETRIEVER_QUEUE = "queue:retriever"
    ANALYZER_QUEUE = "queue:analyzer"
    WRITER_QUEUE = "queue:writer"
    WRITER_CHUNKS_QUEUE = "queue:writer:chunks"
    RESULTS_QUEUE = "queue:results"
    DEAD_LETTER_QUEUE = "queue:dlq"
    
    # Stream names for progress updates
    PROGRESS_STREAM = "stream:progress"
    
    # Retry configuration
    MAX_RETRIES = settings.queue_retry_max_attempts
    BACKOFF_BASE = settings.queue_retry_backoff_base
    MESSAGE_TTL = settings.queue_message_ttl


class RedisQueueClient:
    """
    Async Redis client for queue operations.
    Handles connection pooling, serialization, and error handling.
    """
    
    def __init__(self):
        self._redis: Optional[Redis] = None
        self._connection_params = {
            "host": settings.redis_host,
            "port": settings.redis_port,
            "db": settings.redis_db,
            "password": settings.redis_password,
            "decode_responses": True,
            "encoding": "utf-8",
        }
    
    async def connect(self) -> None:
        """Establish Redis connection."""
        if self._redis is None:
            self._redis = await aioredis.Redis(**self._connection_params)
            logger.info(f"Connected to Redis at {settings.redis_host}:{settings.redis_port}")
    
    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()
            self._redis = None
            logger.info("Disconnected from Redis")
    
    async def enqueue(
        self,
        queue_name: str,
        message: BaseModel,
        priority: int = 0
    ) -> bool:
        """
        Add a message to a queue.
        
        Args:
            queue_name: Target queue name
            message: Pydantic message model
            priority: Message priority (higher = more important)
            
        Returns:
            True if successful
        """
        try:
            await self.connect()
            
            # Serialize message
            payload = message.model_dump_json()
            
            # Use sorted set for priority queue (lower score = higher priority)
            score = -priority
            await self._redis.zadd(queue_name, {payload: score})
            
            logger.debug(f"Enqueued message to {queue_name}: {message.model_dump().get('correlation_id', 'N/A')}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to enqueue message to {queue_name}: {e}")
            return False
    
    async def dequeue(
        self,
        queue_name: str,
        timeout: int = 0
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve and remove a message from queue.
        
        Args:
            queue_name: Source queue name
            timeout: Block for N seconds if queue is empty (0 = no wait)
            
        Returns:
            Deserialized message dict or None
        """
        try:
            await self.connect()
            
            # Get highest priority message (lowest score) - compatible with Redis 3.x
            result = await self._redis.zrange(queue_name, 0, 0, withscores=True)
            
            if not result:
                if timeout > 0:
                    # Simple polling for blocking behavior
                    await asyncio.sleep(1)
                    return await self.dequeue(queue_name, timeout - 1)
                return None
            
            payload, score = result[0]
            # Remove the message from the sorted set
            await self._redis.zrem(queue_name, payload)
            
            message = json.loads(payload)
            
            logger.debug(f"Dequeued message from {queue_name}: {message.get('correlation_id', 'N/A')}")
            return message
            
        except Exception as e:
            logger.error(f"Failed to dequeue from {queue_name}: {e}")
            return None
    
    async def publish_stream(
        self,
        stream_name: str,
        data: Dict[str, Any],
        maxlen: int = 1000
    ) -> Optional[str]:
        """
        Publish data to a Redis channel (pubsub) for broadcasting.
        Uses PUBLISH instead of XADD for Redis 3.x compatibility.
        
        Args:
            stream_name: Channel name
            data: Data dictionary to publish
            maxlen: Ignored (kept for API compatibility)
            
        Returns:
            Number of subscribers that received the message, or None
        """
        try:
            await self.connect()
            
            # Serialize data with datetime support
            payload = json.dumps(data, cls=DateTimeEncoder)
            
            # Use PUBLISH instead of XADD for old Redis compatibility
            result = await self._redis.publish(stream_name, payload)
            
            logger.debug(f"Published to channel {stream_name}: {result} subscribers")
            return str(result)
            
        except Exception as e:
            logger.error(f"Failed to publish to channel {stream_name}: {e}")
            return None
    
    async def read_stream(
        self,
        stream_name: str,
        last_id: str = "0",
        count: int = 10,
        block: int = 1000
    ) -> list:
        """
        Read from a Redis stream.
        
        Args:
            stream_name: Stream to read from
            last_id: Read messages after this ID
            count: Max messages to read
            block: Block for N milliseconds if no data
            
        Returns:
            List of messages
        """
        try:
            await self.connect()
            
            messages = await self._redis.xread(
                {stream_name: last_id},
                count=count,
                block=block
            )
            
            return messages
            
        except Exception as e:
            logger.error(f"Failed to read from stream {stream_name}: {e}")
            return []
    
    async def move_to_dlq(
        self,
        message: Dict[str, Any],
        queue_name: str,
        reason: str
    ) -> bool:
        """
        Move a failed message to the dead-letter queue.
        
        Args:
            message: Original message
            queue_name: Source queue name
            reason: Failure reason
            
        Returns:
            True if successful
        """
        try:
            from schemas import DeadLetterMessage
            
            dlq_msg = DeadLetterMessage(
                original_message=message,
                queue_name=queue_name,
                failure_reason=reason,
                retry_count=message.get("retry_count", 0)
            )
            
            await self.enqueue(QueueConfig.DEAD_LETTER_QUEUE, dlq_msg)
            logger.warning(f"Moved message to DLQ: {reason}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to move message to DLQ: {e}")
            return False
    
    async def set_with_ttl(
        self,
        key: str,
        value: Any,
        ttl: int = QueueConfig.MESSAGE_TTL
    ) -> bool:
        """
        Set a key-value pair with TTL.
        
        Args:
            key: Redis key
            value: Value (will be JSON serialized if not string)
            ttl: Time to live in seconds
            
        Returns:
            True if successful
        """
        try:
            await self.connect()
            
            if not isinstance(value, str):
                value = json.dumps(value, cls=DateTimeEncoder)
            
            await self._redis.setex(key, ttl, value)
            return True
            
        except Exception as e:
            logger.error(f"Failed to set key {key}: {e}")
            return False
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get value by key.
        
        Args:
            key: Redis key
            
        Returns:
            Deserialized value or None
        """
        try:
            await self.connect()
            
            value = await self._redis.get(key)
            if value:
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return None
            
        except Exception as e:
            logger.error(f"Failed to get key {key}: {e}")
            return None


# Global queue client instance
queue_client = RedisQueueClient()


async def retry_with_backoff(
    func: Callable,
    max_retries: int = QueueConfig.MAX_RETRIES,
    backoff_base: int = QueueConfig.BACKOFF_BASE,
    *args,
    **kwargs
) -> Any:
    """
    Execute a function with exponential backoff retry logic.
    
    Args:
        func: Async function to execute
        max_retries: Maximum retry attempts
        backoff_base: Base for exponential backoff
        *args, **kwargs: Arguments for the function
        
    Returns:
        Function result
        
    Raises:
        Last exception if all retries fail
    """
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = backoff_base ** attempt
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"All {max_retries} attempts failed")
    
    raise last_exception
