"""
Writer Agent - Generates user-facing output with streaming support.
Supports: Groq, OpenAI, or mock generation.
"""
import logging
import asyncio
import os
from typing import Dict, Any, AsyncGenerator
import json

from agents.base_agent import BaseAgent
from schemas import WriteMessage, WriteResult, WriteChunk
from queues import queue_client, QueueConfig
from config import settings

logger = logging.getLogger(__name__)

# Try to import OpenAI (works for Groq too since it's OpenAI-compatible)
try:
    from openai import AsyncOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logger.warning("OpenAI package not installed - using mock generation")


class WriterAgent(BaseAgent):
    """
    Generates final user-facing output.
    
    Supports multiple LLM providers:
    - Groq: Set GROQ_API_KEY (fast inference with Llama, Mixtral)
    - OpenAI: Set OPENAI_API_KEY
    - Mock: No API key (template-based responses)
    """
    
    def __init__(self):
        super().__init__(
            name="WriterAgent",
            input_queue=QueueConfig.WRITER_QUEUE,
            timeout=settings.writer_timeout
        )
        
        self.llm_client = None
        self.llm_model = None
        self.llm_provider = None
        
        # Check for Groq API key first
        groq_key = os.environ.get("GROQ_API_KEY")
        openai_key = os.environ.get("OPENAI_API_KEY")
        
        if OPENAI_AVAILABLE and groq_key:
            # Use Groq - OpenAI-compatible API
            self.llm_client = AsyncOpenAI(
                api_key=groq_key,
                base_url="https://api.groq.com/openai/v1"
            )
            self.llm_model = "llama-3.3-70b-versatile"  # Updated model
            self.llm_provider = "Groq"
            logger.info("WriterAgent initialized with Groq (Llama 3.3 70B)")
            
        elif OPENAI_AVAILABLE and openai_key:
            # Use OpenAI
            self.llm_client = AsyncOpenAI(api_key=openai_key)
            self.llm_model = "gpt-3.5-turbo"
            self.llm_provider = "OpenAI"
            logger.info("WriterAgent initialized with OpenAI")
            
        else:
            logger.info("WriterAgent using mock generation")
            logger.info("  Set GROQ_API_KEY for Groq or OPENAI_API_KEY for OpenAI")
    
    async def process(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate written output, optionally streaming.
        
        Args:
            message: WriteMessage dictionary
            
        Returns:
            WriteResult dictionary
        """
        correlation_id = message["correlation_id"]
        step_id = message["step_id"]
        prompt = message["prompt"]
        context = message["context"]
        should_stream = message.get("stream", True)
        
        logger.info(f"Generating output for task: {correlation_id}")
        
        try:
            # Generate content
            if should_stream:
                content = await self._generate_with_streaming(
                    correlation_id, 
                    step_id, 
                    prompt, 
                    context
                )
            else:
                content = await self._generate(prompt, context)
            
            result = WriteResult(
                correlation_id=correlation_id,
                step_id=step_id,
                content=content,
                metadata={
                    "word_count": len(content.split()),
                    "char_count": len(content)
                },
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
            logger.error(f"Writing failed: {e}", exc_info=True)
            
            error_result = WriteResult(
                correlation_id=correlation_id,
                step_id=step_id,
                content="",
                success=False,
                error=str(e)
            )
            
            await queue_client.enqueue(
                QueueConfig.RESULTS_QUEUE,
                error_result
            )
            
            raise
    
    async def _generate_with_streaming(
        self,
        correlation_id: str,
        step_id: str,
        prompt: str,
        context: Dict[str, Any]
    ) -> str:
        """
        Generate content with streaming chunks.
        
        Args:
            correlation_id: Task ID
            step_id: Step ID
            prompt: Generation prompt
            context: Context data
            
        Returns:
            Complete generated content
        """
        full_content = []
        
        async for chunk in self._stream_generate(prompt, context):
            full_content.append(chunk)
            
            # Publish chunk to streaming queue
            chunk_msg = WriteChunk(
                correlation_id=correlation_id,
                step_id=step_id,
                chunk=chunk,
                is_final=False
            )
            
            await queue_client.enqueue(
                QueueConfig.WRITER_CHUNKS_QUEUE,
                chunk_msg
            )
            
            # Also publish to progress stream
            await queue_client.publish_stream(
                QueueConfig.PROGRESS_STREAM,
                {
                    "task_id": correlation_id,
                    "type": "chunk",
                    "data": chunk
                }
            )
        
        # Send final chunk marker
        final_chunk = WriteChunk(
            correlation_id=correlation_id,
            step_id=step_id,
            chunk="",
            is_final=True
        )
        await queue_client.enqueue(
            QueueConfig.WRITER_CHUNKS_QUEUE,
            final_chunk
        )
        
        return "".join(full_content)
    
    async def _stream_generate(
        self,
        prompt: str,
        context: Dict[str, Any]
    ) -> AsyncGenerator[str, None]:
        """
        Generate content in streaming chunks.
        
        Uses OpenAI if available, otherwise falls back to mock.
        
        Args:
            prompt: Generation prompt
            context: Context data
            
        Yields:
            Content chunks
        """
        # Build comprehensive prompt
        full_prompt = self._build_prompt(prompt, context)
        
        # Use LLM client if available (Grok or OpenAI)
        if self.llm_client:
            try:
                stream = await self.llm_client.chat.completions.create(
                    model=self.llm_model,
                    messages=[
                        {"role": "system", "content": "You are a helpful AI assistant. Provide detailed, well-structured responses."},
                        {"role": "user", "content": full_prompt}
                    ],
                    stream=True,
                    max_tokens=1000
                )
                
                async for chunk in stream:
                    if chunk.choices[0].delta.content:
                        yield chunk.choices[0].delta.content
                return
                
            except Exception as e:
                logger.error(f"{self.llm_provider} error, falling back to mock: {e}")
        
        # Fallback to mock generation
        mock_response = self._mock_generate(full_prompt)
        
        # Simulate streaming by yielding words with delays
        words = mock_response.split()
        for i, word in enumerate(words):
            chunk = word + (" " if i < len(words) - 1 else "")
            yield chunk
            await asyncio.sleep(0.05)  # Simulate network latency
    
    async def _generate(
        self,
        prompt: str,
        context: Dict[str, Any]
    ) -> str:
        """
        Generate content without streaming.
        
        Args:
            prompt: Generation prompt
            context: Context data
            
        Returns:
            Generated content
        """
        full_prompt = self._build_prompt(prompt, context)
        
        # Simulate async processing
        await asyncio.sleep(1)
        
        return self._mock_generate(full_prompt)
    
    def _build_prompt(
        self,
        instruction: str,
        context: Dict[str, Any]
    ) -> str:
        """
        Build comprehensive prompt from instruction and context.
        
        Args:
            instruction: User instruction
            context: Retrieved context and analysis
            
        Returns:
            Complete prompt
        """
        prompt_parts = [
            "You are a helpful AI assistant. Generate a comprehensive response based on the following:\n",
            f"\nUser Request: {instruction}\n",
        ]
        
        # Add retrieved documents if available
        if "documents" in context:
            prompt_parts.append("\nRelevant Information:")
            for doc in context["documents"]:
                prompt_parts.append(f"\n- {doc.get('title', 'Document')}: {doc.get('content', '')}")
        
        # Add analysis insights if available
        if "insights" in context:
            prompt_parts.append(f"\n\nAnalysis Insights: {json.dumps(context['insights'], indent=2)}")
        
        prompt_parts.append("\n\nGenerate a detailed, well-structured response:")
        
        return "".join(prompt_parts)
    
    def _mock_generate(self, prompt: str) -> str:
        """
        Mock content generation - provides task-aware responses.
        
        In production, replace with actual LLM API call.
        Set OPENAI_API_KEY environment variable to use real LLM.
        
        Args:
            prompt: Full prompt
            
        Returns:
            Generated content
        """
        prompt_lower = prompt.lower()
        
        # Extract the user request from the prompt
        if "User Request:" in prompt:
            user_request = prompt.split("User Request:")[1].split("\n")[0].strip()
        else:
            user_request = prompt[:200]
        
        # Haiku detection
        if "haiku" in prompt_lower:
            return """Here's a haiku about debugging:

    Cursor blinks at night
    Semicolon was missing
    Coffee grows cold now

This captures the essence of late-night debugging sessions - the patience required, the often trivial solutions, and the ever-present coffee that accompanies programmers through their trials."""

        # API/REST detection
        if "api" in prompt_lower or "rest" in prompt_lower:
            return f"""# REST API Design for: {user_request}

## Endpoints

### GET /items
- **Description**: Retrieve all items
- **Response**: `200 OK` with array of items

### GET /items/:id
- **Description**: Retrieve a single item by ID
- **Response**: `200 OK` with item object, `404 Not Found` if not exists

### POST /items
- **Description**: Create a new item
- **Request Body**: `{{ "name": "string", "description": "string" }}`
- **Response**: `201 Created` with created item

### PUT /items/:id
- **Description**: Update an existing item
- **Request Body**: `{{ "name": "string", "description": "string" }}`
- **Response**: `200 OK` with updated item

### DELETE /items/:id
- **Description**: Delete an item
- **Response**: `204 No Content`

## Example Request
```bash
curl -X POST http://api.example.com/items \\
  -H "Content-Type: application/json" \\
  -d '{{"name": "Task 1", "description": "My first task"}}'
```"""

        # Fibonacci detection
        if "fibonacci" in prompt_lower:
            return """# Fibonacci Implementation & Analysis

## Recursive Implementation (O(2^n))
```python
def fib_recursive(n):
    if n <= 1:
        return n
    return fib_recursive(n-1) + fib_recursive(n-2)
```
**Time Complexity**: O(2^n) - exponential, very slow for large n
**Space Complexity**: O(n) - call stack depth

## Iterative Implementation (O(n))
```python
def fib_iterative(n):
    if n <= 1:
        return n
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b
```
**Time Complexity**: O(n) - linear
**Space Complexity**: O(1) - constant

## Comparison
| n | Recursive Time | Iterative Time |
|---|----------------|----------------|
| 10 | ~0.1ms | ~0.001ms |
| 30 | ~500ms | ~0.001ms |
| 40 | ~60s | ~0.001ms |

**Recommendation**: Always use iterative for production code."""

        # Compare/vs detection
        if "compare" in prompt_lower or " vs " in prompt_lower or "versus" in prompt_lower:
            return f"""# Comparison Analysis: {user_request}

## Overview
This comparison examines the key aspects of both options to help you make an informed decision.

## Criteria Analysis

### 1. Performance
- **Option A**: Generally faster for small-scale applications
- **Option B**: Better optimized for large-scale deployments

### 2. Learning Curve
- **Option A**: Easier to get started, more intuitive API
- **Option B**: Steeper learning curve but more powerful features

### 3. Ecosystem & Community
- **Option A**: Large community, extensive documentation
- **Option B**: Growing community, improving documentation

### 4. Use Cases
- **Option A**: Best for rapid prototyping, small-medium projects
- **Option B**: Best for enterprise applications, complex requirements

## Recommendation
For startups and MVPs: Start with the simpler option and migrate later if needed.
For enterprise: Invest in the more robust solution from the start.

The best choice depends on your specific requirements, team expertise, and project timeline."""

        # Database/schema detection
        if "database" in prompt_lower or "schema" in prompt_lower:
            return f"""# Database Schema Design: {user_request}

## Entity Relationship Diagram

```
┌─────────────┐       ┌─────────────┐       ┌─────────────┐
│   Users     │       │   Orders    │       │  Products   │
├─────────────┤       ├─────────────┤       ├─────────────┤
│ id (PK)     │──┐    │ id (PK)     │    ┌──│ id (PK)     │
│ email       │  │    │ user_id(FK) │◄───┤  │ name        │
│ name        │  └───►│ total       │    │  │ price       │
│ created_at  │       │ status      │    │  │ stock       │
└─────────────┘       │ created_at  │    │  └─────────────┘
                      └─────────────┘    │
                            │            │
                      ┌─────┴─────┐      │
                      │OrderItems │      │
                      ├───────────┤      │
                      │ id (PK)   │      │
                      │ order_id  │◄─────┘
                      │ product_id│
                      │ quantity  │
                      │ price     │
                      └───────────┘
```

## SQL Schema
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock INTEGER DEFAULT 0
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    total DECIMAL(10,2),
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);
```"""

        # Default response for any other task
        return f"""# Response to: {user_request}

## Analysis

Based on your request, here's a comprehensive response:

### Key Points

1. **Understanding the Problem**
   - The task requires careful consideration of multiple factors
   - Best practices should be followed for optimal results

2. **Recommended Approach**
   - Start with a clear definition of requirements
   - Break down the problem into manageable components
   - Implement iteratively with testing at each stage

3. **Implementation Considerations**
   - Consider scalability from the beginning
   - Document decisions for future reference
   - Plan for error handling and edge cases

### Next Steps

1. Define specific requirements and constraints
2. Create a detailed implementation plan
3. Set up necessary infrastructure
4. Begin development with core functionality
5. Add features incrementally

### Conclusion

This task can be accomplished effectively by following a structured approach. 
The key is to maintain clarity in objectives and adapt as requirements evolve.

---
*Note: This is a demonstration response. Set OPENAI_API_KEY for AI-generated content.*"""
