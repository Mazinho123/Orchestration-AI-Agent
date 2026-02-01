"""
Base agent class with common functionality.
All specialized agents inherit from this class.
"""
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from datetime import datetime

from schemas import BaseMessage
from queues import queue_client, QueueConfig

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """
    Abstract base class for all agents.
    Handles queue consumption, error handling, and result publishing.
    """
    
    def __init__(self, name: str, input_queue: str, timeout: int = 30):
        """
        Initialize the agent.
        
        Args:
            name: Agent name for logging
            input_queue: Queue to consume messages from
            timeout: Processing timeout in seconds
        """
        self.name = name
        self.input_queue = input_queue
        self.timeout = timeout
        self._running = False
        
    @abstractmethod
    async def process(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a message and return the result.
        Must be implemented by subclasses.
        
        Args:
            message: Input message dictionary
            
        Returns:
            Result dictionary
        """
        pass
    
    async def run(self) -> None:
        """
        Main agent loop - consumes and processes messages.
        Runs indefinitely until stopped.
        """
        self._running = True
        logger.info(f"{self.name} agent started, listening on {self.input_queue}")
        
        while self._running:
            try:
                # Poll for messages
                message = await queue_client.dequeue(self.input_queue, timeout=1)
                
                if message is None:
                    await asyncio.sleep(0.1)  # Brief sleep to avoid tight loop
                    continue
                
                # Process with timeout
                try:
                    result = await asyncio.wait_for(
                        self._process_with_retry(message),
                        timeout=self.timeout
                    )
                    logger.info(f"{self.name} processed message {message.get('correlation_id', 'N/A')}")
                    
                except asyncio.TimeoutError:
                    logger.error(f"{self.name} timeout processing message {message.get('correlation_id', 'N/A')}")
                    await self._handle_failure(message, "Processing timeout")
                    
            except Exception as e:
                logger.error(f"{self.name} error in main loop: {e}", exc_info=True)
                await asyncio.sleep(1)  # Brief pause before continuing
    
    async def _process_with_retry(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process message with retry logic.
        
        Args:
            message: Input message
            
        Returns:
            Processing result
        """
        retry_count = message.get("retry_count", 0)
        max_retries = QueueConfig.MAX_RETRIES
        
        try:
            # Attempt processing
            result = await self.process(message)
            return result
            
        except Exception as e:
            logger.error(f"{self.name} processing error: {e}", exc_info=True)
            
            # Check if we should retry
            if retry_count < max_retries:
                # Increment retry count and re-enqueue
                message["retry_count"] = retry_count + 1
                backoff_delay = QueueConfig.BACKOFF_BASE ** retry_count
                
                logger.warning(f"{self.name} retrying message (attempt {retry_count + 1}/{max_retries}) in {backoff_delay}s")
                
                await asyncio.sleep(backoff_delay)
                await queue_client.enqueue(self.input_queue, self._dict_to_message(message))
                
            else:
                # Max retries exceeded - move to DLQ
                await self._handle_failure(message, str(e))
            
            raise
    
    async def _handle_failure(self, message: Dict[str, Any], reason: str) -> None:
        """
        Handle message processing failure.
        
        Args:
            message: Failed message
            reason: Failure reason
        """
        logger.error(f"{self.name} permanent failure: {reason}")
        await queue_client.move_to_dlq(message, self.input_queue, reason)
    
    def _dict_to_message(self, data: Dict[str, Any]) -> BaseMessage:
        """
        Convert dictionary back to BaseMessage for re-enqueueing.
        
        Args:
            data: Message dictionary
            
        Returns:
            BaseMessage instance
        """
        return BaseMessage(
            correlation_id=data["correlation_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            retry_count=data.get("retry_count", 0)
        )
    
    def stop(self) -> None:
        """Stop the agent gracefully."""
        logger.info(f"Stopping {self.name} agent")
        self._running = False
