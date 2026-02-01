"""
FastAPI application with SSE streaming endpoints.
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator
import uuid
import json

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from config import settings
from schemas import TaskRequest, ProgressUpdate, TaskStatus
from orchestrator import orchestrator
from queues import queue_client, QueueConfig
from agents import PlannerAgent, RetrieverAgent, AnalyzerAgent, WriterAgent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Background task runners for agents
agent_tasks = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    Starts agents and orchestrator on startup, cleans up on shutdown.
    """
    logger.info("Starting Agentic AI System...")
    
    try:
        # Initialize Redis connection
        await queue_client.connect()
        
        # Start orchestrator
        await orchestrator.start()
        
        # Start all agents in background (don't wait for them)
        planner = PlannerAgent()
        retriever = RetrieverAgent()
        analyzer = AnalyzerAgent()
        writer = WriterAgent()
        
        # Create tasks but don't add to agent_tasks list to avoid issues
        asyncio.create_task(planner.run())
        asyncio.create_task(retriever.run())
        asyncio.create_task(analyzer.run())
        asyncio.create_task(writer.run())
        
        logger.info("All agents started successfully")
        
        yield
        
    except Exception as e:
        logger.error(f"Startup error: {e}", exc_info=True)
        raise
    finally:
        # Cleanup on shutdown
        try:
            logger.info("Shutting down...")
            orchestrator.stop()
            await queue_client.disconnect()
            logger.info("Shutdown complete")
        except Exception as e:
            logger.error(f"Shutdown error: {e}", exc_info=True)


# Create FastAPI app
app = FastAPI(
    title="Agentic AI System",
    description="Production-grade multi-agent system with async orchestration",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request/Response models
class TaskSubmitRequest(BaseModel):
    """Request model for task submission."""
    instruction: str
    context: dict = {}


class TaskSubmitResponse(BaseModel):
    """Response model for task submission."""
    task_id: str
    message: str
    stream_url: str


class TaskStatusResponse(BaseModel):
    """Response model for task status."""
    task_id: str
    status: str
    completed_steps: int
    total_steps: int


# API Endpoints

@app.get("/")
async def root():
    """Root endpoint - API info."""
    return {
        "name": "Agentic AI System",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "submit_task": "POST /task",
            "stream_progress": "GET /stream/{task_id}",
            "get_status": "GET /status/{task_id}",
            "health": "GET /health"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Check Redis connection
        await queue_client.connect()
        redis_status = "healthy"
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        redis_status = "unhealthy"
    
    return {
        "status": "healthy" if redis_status == "healthy" else "degraded",
        "components": {
            "redis": redis_status,
            "orchestrator": "running",
            "agents": "running"
        }
    }


@app.post("/task", response_model=TaskSubmitResponse)
async def submit_task(request: TaskSubmitRequest):
    """
    Submit a complex task for processing.
    
    The task will be decomposed into steps and executed asynchronously
    by specialized agents. Progress can be monitored via the stream endpoint.
    
    Args:
        request: Task submission request
        
    Returns:
        Task ID and stream URL
    """
    try:
        # Generate unique task ID
        task_id = f"task_{uuid.uuid4().hex[:12]}"
        
        # Create task request
        task = TaskRequest(
            task_id=task_id,
            user_instruction=request.instruction,
            context=request.context
        )
        
        # Submit to orchestrator
        success = await orchestrator.submit_task(task)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to submit task")
        
        logger.info(f"Task submitted: {task_id}")
        
        return TaskSubmitResponse(
            task_id=task_id,
            message="Task submitted successfully",
            stream_url=f"/stream/{task_id}"
        )
        
    except Exception as e:
        logger.error(f"Error submitting task: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status/{task_id}")
async def get_task_status(task_id: str):
    """
    Get current status of a task.
    
    Args:
        task_id: Task ID
        
    Returns:
        Task status information
    """
    status = await orchestrator.get_task_status(task_id)
    
    if status is None:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return status


@app.get("/tasks/{task_id}")
async def get_task_result(task_id: str):
    """
    Get task result including final output.
    
    Args:
        task_id: Task ID
        
    Returns:
        Task status and output
    """
    status = await orchestrator.get_task_status(task_id)
    
    if status is None:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return status


@app.get("/stream/{task_id}")
async def stream_task_progress(task_id: str):
    """
    Stream real-time progress updates for a task using Server-Sent Events.
    Streams actual task progress and output chunks.
    """
    
    async def event_generator() -> AsyncGenerator[str, None]:
        """Generate SSE events with real task data."""
        try:
            # Send connection confirmation
            yield f"event: connected\ndata: {json.dumps({'task_id': task_id})}\n\n"
            
            last_status = None
            last_completed = -1
            timeout_count = 0
            max_timeout = 120  # 2 minutes max
            
            while timeout_count < max_timeout:
                await asyncio.sleep(0.5)
                timeout_count += 1
                
                # Get current task status
                status = await orchestrator.get_task_status(task_id)
                
                if status is None:
                    yield f"event: error\ndata: {json.dumps({'error': 'Task not found'})}\n\n"
                    break
                
                current_status = status.get('status')
                completed_steps = status.get('completed_steps', 0)
                
                # Send update if status or progress changed
                if current_status != last_status or completed_steps != last_completed:
                    yield f"event: progress\ndata: {json.dumps(status)}\n\n"
                    last_status = current_status
                    last_completed = completed_steps
                
                # Check if task completed or failed
                if current_status in ['completed', 'failed']:
                    # Get final output
                    output = status.get('output', '')
                    yield f"event: complete\ndata: {json.dumps({'status': current_status, 'output': output})}\n\n"
                    break
                
                # Send keepalive every 10 seconds
                if timeout_count % 20 == 0:
                    yield f"event: keepalive\ndata: {json.dumps({'timestamp': timeout_count})}\n\n"
            
        except Exception as e:
            logger.error(f"SSE error: {e}")
            yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive"
        }
    )


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_level="info"
    )
