"""
Message and data schemas for the Agentic AI System.
Defines all message types passed between agents via Redis queues.
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Literal
from datetime import datetime
from enum import Enum


class AgentType(str, Enum):
    """Enumeration of available agent types."""
    PLANNER = "planner"
    RETRIEVER = "retriever"
    ANALYZER = "analyzer"
    WRITER = "writer"


class TaskStatus(str, Enum):
    """Task execution status."""
    PENDING = "pending"
    PLANNING = "planning"
    RETRIEVING = "retrieving"
    ANALYZING = "analyzing"
    WRITING = "writing"
    COMPLETED = "completed"
    FAILED = "failed"


class StepType(str, Enum):
    """Types of execution steps."""
    RETRIEVE = "retrieve"
    ANALYZE = "analyze"
    WRITE = "write"


class BaseMessage(BaseModel):
    """Base message schema for all queue messages."""
    correlation_id: str = Field(..., description="Unique task identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    retry_count: int = Field(default=0, ge=0)
    

class TaskRequest(BaseModel):
    """Initial user task request."""
    task_id: str
    user_instruction: str = Field(..., min_length=1)
    context: Optional[Dict[str, Any]] = None
    

class ExecutionStep(BaseModel):
    """A single step in the execution plan."""
    step_id: str
    step_type: StepType
    agent: AgentType
    description: str
    depends_on: Optional[List[str]] = None  # Step IDs this depends on
    parameters: Dict[str, Any] = Field(default_factory=dict)


class PlanMessage(BaseMessage):
    """Message for the Planner agent."""
    user_instruction: str
    context: Optional[Dict[str, Any]] = None


class PlanResult(BaseModel):
    """Result from the Planner agent."""
    correlation_id: str
    steps: List[ExecutionStep]
    estimated_duration: Optional[int] = None  # seconds
    success: bool = True
    error: Optional[str] = None


class RetrieveMessage(BaseMessage):
    """Message for the Retriever agent."""
    step_id: str
    query: str
    sources: Optional[List[str]] = None
    max_results: int = 5


class RetrievalResult(BaseModel):
    """Result from the Retriever agent."""
    correlation_id: str
    step_id: str
    documents: List[Dict[str, Any]]
    success: bool = True
    error: Optional[str] = None


class AnalyzeMessage(BaseMessage):
    """Message for the Analyzer agent."""
    step_id: str
    data: Dict[str, Any]
    analysis_type: str
    parameters: Dict[str, Any] = Field(default_factory=dict)


class AnalysisResult(BaseModel):
    """Result from the Analyzer agent."""
    correlation_id: str
    step_id: str
    insights: Dict[str, Any]
    confidence: Optional[float] = None
    success: bool = True
    error: Optional[str] = None


class WriteMessage(BaseMessage):
    """Message for the Writer agent."""
    step_id: str
    prompt: str
    context: Dict[str, Any]
    stream: bool = True


class WriteChunk(BaseModel):
    """Streaming chunk from the Writer agent."""
    correlation_id: str
    step_id: str
    chunk: str
    is_final: bool = False


class WriteResult(BaseModel):
    """Final result from the Writer agent."""
    correlation_id: str
    step_id: str
    content: str
    metadata: Optional[Dict[str, Any]] = None
    success: bool = True
    error: Optional[str] = None


class ProgressUpdate(BaseModel):
    """Progress update message for streaming to client."""
    task_id: str
    status: TaskStatus
    message: str
    step_id: Optional[str] = None
    progress_percent: Optional[int] = Field(None, ge=0, le=100)
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class TaskResult(BaseModel):
    """Final task execution result."""
    task_id: str
    status: TaskStatus
    output: Optional[str] = None
    steps_completed: List[str] = Field(default_factory=list)
    error: Optional[str] = None
    duration: Optional[float] = None  # seconds
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class DeadLetterMessage(BaseModel):
    """Message moved to dead-letter queue after max retries."""
    original_message: Dict[str, Any]
    queue_name: str
    failure_reason: str
    retry_count: int
    timestamp: datetime = Field(default_factory=datetime.utcnow)
