"""
Retriever Agent - Fetches external and internal context.
"""
import logging
import asyncio
from typing import Dict, Any, List
import random

from agents.base_agent import BaseAgent
from schemas import RetrieveMessage, RetrievalResult
from queues import queue_client, QueueConfig
from config import settings

logger = logging.getLogger(__name__)


class RetrieverAgent(BaseAgent):
    """
    Retrieves relevant context and information from various sources.
    In production, this would integrate with:
    - Vector databases (Pinecone, Weaviate, etc.)
    - Search APIs (Google, Bing, etc.)
    - Internal knowledge bases
    - Document stores
    """
    
    def __init__(self):
        super().__init__(
            name="RetrieverAgent",
            input_queue=QueueConfig.RETRIEVER_QUEUE,
            timeout=settings.retriever_timeout
        )
        
        # Mock knowledge base for demonstration
        self.knowledge_base = self._init_mock_knowledge_base()
    
    async def process(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Retrieve relevant documents based on query.
        
        Args:
            message: RetrieveMessage dictionary
            
        Returns:
            RetrievalResult dictionary
        """
        correlation_id = message["correlation_id"]
        step_id = message["step_id"]
        query = message["query"]
        max_results = message.get("max_results", 5)
        
        logger.info(f"Retrieving documents for: {query}")
        
        try:
            # Perform retrieval
            documents = await self._retrieve_documents(query, max_results)
            
            result = RetrievalResult(
                correlation_id=correlation_id,
                step_id=step_id,
                documents=documents,
                success=True
            )
            
            # Publish result
            await queue_client.enqueue(
                QueueConfig.RESULTS_QUEUE,
                result
            )
            
            # Store result for later access
            await queue_client.set_with_ttl(
                f"result:{correlation_id}:{step_id}",
                result.model_dump()
            )
            
            return result.model_dump()
            
        except Exception as e:
            logger.error(f"Retrieval failed: {e}", exc_info=True)
            
            error_result = RetrievalResult(
                correlation_id=correlation_id,
                step_id=step_id,
                documents=[],
                success=False,
                error=str(e)
            )
            
            await queue_client.enqueue(
                QueueConfig.RESULTS_QUEUE,
                error_result
            )
            
            raise
    
    async def _retrieve_documents(
        self, 
        query: str, 
        max_results: int
    ) -> List[Dict[str, Any]]:
        """
        Retrieve relevant documents.
        
        In production, this would:
        1. Embed the query using a model
        2. Perform vector similarity search
        3. Re-rank results
        4. Return top K documents
        
        Args:
            query: Search query
            max_results: Maximum documents to return
            
        Returns:
            List of document dictionaries
        """
        # Simulate async retrieval with delay
        await asyncio.sleep(0.5)
        
        # Mock retrieval - in production, replace with real search
        query_lower = query.lower()
        relevant_docs = []
        
        for doc in self.knowledge_base:
            # Simple keyword matching
            if any(term in doc["content"].lower() for term in query_lower.split()):
                relevant_docs.append(doc)
        
        # Add relevance scores (mock)
        for doc in relevant_docs:
            doc["relevance_score"] = random.uniform(0.7, 0.99)
        
        # Sort by relevance and limit
        relevant_docs.sort(key=lambda x: x["relevance_score"], reverse=True)
        return relevant_docs[:max_results]
    
    def _init_mock_knowledge_base(self) -> List[Dict[str, Any]]:
        """
        Initialize mock knowledge base.
        In production, this would connect to actual data sources.
        
        Returns:
            List of mock documents
        """
        return [
            {
                "id": "doc_1",
                "title": "Introduction to AI Agents",
                "content": "AI agents are autonomous entities that perceive their environment and take actions to achieve goals. They can be reactive, deliberative, or hybrid.",
                "source": "knowledge_base",
                "metadata": {"category": "ai_fundamentals"}
            },
            {
                "id": "doc_2",
                "title": "Multi-Agent Systems",
                "content": "Multi-agent systems coordinate multiple AI agents to solve complex problems. Each agent may have specialized capabilities and communicate via message passing.",
                "source": "knowledge_base",
                "metadata": {"category": "distributed_ai"}
            },
            {
                "id": "doc_3",
                "title": "Async Programming Patterns",
                "content": "Asynchronous programming allows non-blocking I/O operations. Python's asyncio provides coroutines, tasks, and event loops for concurrent execution.",
                "source": "knowledge_base",
                "metadata": {"category": "programming"}
            },
            {
                "id": "doc_4",
                "title": "Message Queue Architecture",
                "content": "Message queues enable decoupled communication between services. Redis, RabbitMQ, and Kafka are popular choices for building distributed systems.",
                "source": "knowledge_base",
                "metadata": {"category": "architecture"}
            },
            {
                "id": "doc_5",
                "title": "LLM Integration Best Practices",
                "content": "When integrating large language models, consider token limits, streaming responses, rate limiting, and fallback strategies for production systems.",
                "source": "knowledge_base",
                "metadata": {"category": "llm"}
            },
            {
                "id": "doc_6",
                "title": "Production System Design",
                "content": "Production systems require observability, error handling, graceful degradation, and monitoring. Implement health checks, logging, and metrics.",
                "source": "knowledge_base",
                "metadata": {"category": "engineering"}
            },
        ]
