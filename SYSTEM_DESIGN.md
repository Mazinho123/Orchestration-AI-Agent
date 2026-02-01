# Agentic AI System - System Design Document

## Overview

A production-grade multi-agent AI system that processes complex user tasks through specialized agents coordinated via asynchronous message queues. The system breaks down tasks into steps, distributes them to specialized agents, and streams real-time progress to users.

## Architecture

### High-Level Design

```
User Request
    ↓
FastAPI Server (SSE Streaming)
    ↓
Task Orchestrator
    ↓
Redis Message Queues (Sorted Sets + Pub/Sub)
    ↓
Specialized Agents (Async Workers)
    ↓
LLM Integration (Groq/OpenAI)
    ↓
Streamed Response to User
```

### Core Principles

1. **Asynchronous Processing**: All agent operations are non-blocking
2. **Message Queue Based**: Redis-backed queues for reliable communication
3. **Specialized Agents**: Each agent has a specific responsibility
4. **Real-time Streaming**: SSE for live progress updates
5. **Fault Tolerance**: Retry logic, exponential backoff, dead-letter queues

## System Components

### 1. FastAPI Server (`api/main.py`)

**Responsibilities:**
- Accept user task submissions via REST API
- Stream real-time progress via Server-Sent Events (SSE)
- Manage application lifecycle (startup/shutdown)

**Key Endpoints:**
- `POST /task` - Submit new task
- `GET /tasks/{task_id}` - Get task result
- `GET /status/{task_id}` - Get task status
- `GET /stream/{task_id}` - Stream real-time progress (SSE)
- `GET /health` - Health check

**Technologies:**
- FastAPI (async web framework)
- Uvicorn (ASGI server)
- CORS middleware for cross-origin support

### 2. Task Orchestrator (`orchestrator/task_orchestrator.py`)

**Responsibilities:**
- Decompose user tasks into execution steps
- Manage agent dependencies and execution order
- Track task state and progress
- Coordinate message flow between agents
- Store results in Redis

**Task Flow:**
1. Receives task from API
2. Creates execution plan with dependencies
3. Dispatches steps to appropriate agent queues
4. Monitors completion and publishes progress
5. Aggregates final output

**State Management:**
- Stores task metadata in Redis hash (`task:{task_id}`)
- Tracks: status, steps, completed_steps, output
- Publishes updates to `task_updates` channel

### 3. Specialized Agents

All agents inherit from `BaseAgent` which provides:
- Queue consumption loop
- Retry logic with exponential backoff
- Error handling and dead-letter queue support
- Timeout management

#### a) PlannerAgent (`agents/planner_agent.py`)

**Queue:** `planner_queue`

**Purpose:** Analyzes user instructions and creates execution plan

**Processing:**
- Receives: User instruction + context
- Outputs: List of execution steps with priorities
- Current Implementation: Mock planning (can be enhanced with LLM)

#### b) RetrieverAgent (`agents/retriever_agent.py`)

**Queue:** `retriever_queue`

**Purpose:** Gathers relevant information/context

**Processing:**
- Receives: Query and context requirements
- Outputs: Retrieved context data
- Current Implementation: Mock retrieval (can integrate vector DB, search APIs)

#### c) AnalyzerAgent (`agents/analyzer_agent.py`)

**Queue:** `analyzer_queue`

**Purpose:** Analyzes gathered information

**Processing:**
- Receives: Raw data from retriever
- Outputs: Analyzed insights
- Current Implementation: Mock analysis (can use specialized models)

#### d) WriterAgent (`agents/writer_agent.py`)

**Queue:** `writer_queue`

**Purpose:** Generates final user-facing output

**Processing:**
- Receives: Analysis results + user instruction
- Outputs: AI-generated response
- **LLM Integration:**
  - **Primary:** Groq API (llama-3.3-70b-versatile)
  - **Fallback:** OpenAI API (gpt-3.5-turbo)
  - **Default:** Mock generation

**Environment Variables:**
- `GROQ_API_KEY` - For Groq integration (recommended for speed)
- `OPENAI_API_KEY` - For OpenAI integration

### 4. Message Queue System (`queues/redis_queue.py`)

**Technology:** Redis

**Queue Implementation:**
- **Sorted Sets (ZSET)** for priority queues
- **Pub/Sub** for real-time updates
- **Hashes** for task state storage

**Key Operations:**
```python
enqueue(queue_name, message, priority)  # Add message to queue
dequeue(queue_name, timeout)            # Get next message
move_to_dlq(message, queue, reason)     # Handle failures
publish_update(channel, data)            # Broadcast updates
```

**Retry Logic:**
- Max retries: 3 (configurable)
- Backoff: Exponential (2^retry_count seconds)
- Failed messages → Dead Letter Queue (DLQ)

**Redis Keys:**
```
queue:{queue_name}           # Sorted set for messages
task:{task_id}               # Hash for task state
task:{task_id}:output        # String for final output
dlq:{queue_name}             # Dead letter queue
```

### 5. Configuration (`config.py`)

**Settings (Environment Variables):**
```python
# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# API Server
API_HOST=0.0.0.0
API_PORT=8001
CORS_ORIGINS=*

# Agent Timeouts
PLANNER_TIMEOUT=30
RETRIEVER_TIMEOUT=30
ANALYZER_TIMEOUT=30
WRITER_TIMEOUT=60

# LLM APIs
GROQ_API_KEY=gsk_...
OPENAI_API_KEY=sk-...
```

### 6. Data Schemas (`schemas/`)

**Message Types:**
- `BaseMessage` - Common fields (correlation_id, timestamp, retry_count)
- `TaskRequest` - User task submission
- `PlanMessage` - Planning request
- `RetrieveMessage` - Retrieval request
- `AnalyzeMessage` - Analysis request
- `WriteMessage` - Writing request
- `ProgressUpdate` - Status update
- `TaskStatus` - Task state

All schemas use Pydantic for validation.

## Data Flow

### 1. Task Submission Flow

```
1. User submits task via POST /task
   → FastAPI validates request
   → Generates unique task_id

2. Orchestrator receives task
   → Creates execution plan
   → Stores initial state in Redis
   → Dispatches to PlannerAgent queue

3. PlannerAgent processes
   → Analyzes instruction
   → Creates step plan
   → Returns to orchestrator

4. Orchestrator dispatches steps
   → Enqueues RetrieverAgent task
   → Waits for completion
   → Enqueues AnalyzerAgent task
   → Waits for completion
   → Enqueues WriterAgent task

5. WriterAgent generates output
   → Calls Groq/OpenAI API
   → Stores output in Redis
   → Marks task as completed

6. User retrieves result
   → GET /tasks/{task_id}
   → Returns final output
```

### 2. Real-Time Streaming Flow

```
1. User connects to GET /stream/{task_id}
   → Server sends SSE connection confirmation

2. While task processing:
   → Orchestrator publishes updates to Redis channel
   → SSE endpoint polls task status every 500ms
   → Sends progress events when state changes
   
3. Events sent:
   event: connected     → Connection established
   event: progress      → Status/step updates
   event: complete      → Final output ready
   event: error         → Error occurred
   event: keepalive     → Connection maintenance

4. Client receives real-time updates
   → Displays progress to user
   → Shows final output when complete
```

## Communication Patterns

### 1. Request-Reply Pattern
- Orchestrator → Agent → Orchestrator
- Used for synchronous step execution
- Orchestrator waits for agent response

### 2. Pub-Sub Pattern
- Orchestrator publishes updates → SSE subscribers
- Used for real-time progress streaming
- Multiple clients can subscribe

### 3. Queue-Based Pattern
- Messages added to queues with priority
- Agents consume independently
- Enables horizontal scaling

## Fault Tolerance

### 1. Retry Mechanism
```python
Attempt 1: Immediate processing
Attempt 2: 2 second delay (2^0)
Attempt 3: 4 second delay (2^1)
Attempt 4: 8 second delay (2^2)
Failed: Move to DLQ
```

### 2. Timeout Handling
- Each agent has configurable timeout
- Prevents infinite processing
- Timeouts trigger retry or DLQ

### 3. Dead Letter Queue (DLQ)
- Stores permanently failed messages
- Includes failure reason and metadata
- Can be manually reprocessed or analyzed

### 4. Health Monitoring
- `/health` endpoint checks Redis connectivity
- Agent status tracked in orchestrator
- Server logs all errors with stack traces

## Performance Considerations

### 1. Asynchronous Design
- All I/O operations use `async/await`
- Non-blocking Redis operations
- Concurrent agent processing

### 2. Redis Optimizations
- Sorted sets for O(log N) priority queuing
- Pub/Sub for efficient broadcasting
- Connection pooling (handled by redis-py)

### 3. Streaming Efficiency
- SSE streams only changed data
- Keepalive events prevent timeouts
- Automatic cleanup on completion

### 4. Scalability
- Agents can run on multiple processes/machines
- Horizontal scaling via queue-based architecture
- Stateless agents (state in Redis)

## Security Considerations

### 1. API Keys
- LLM keys stored in environment variables
- Never logged or exposed in responses
- Separate keys per environment

### 2. CORS
- Configurable allowed origins
- Default: all origins (can be restricted)

### 3. Input Validation
- Pydantic schemas validate all inputs
- Type checking and constraint validation
- SQL injection not applicable (using Redis)

### 4. Error Handling
- Stack traces logged but not exposed to users
- Generic error messages in API responses
- Detailed logs for debugging

## Requirements Fulfilled

### ✅ Requirement 1: Accept Complex User Tasks
**Implementation:** `POST /task` endpoint
- Accepts any instruction as string
- Supports additional context as dictionary
- Returns task_id for tracking

### ✅ Requirement 2: Break Into Multiple Steps
**Implementation:** PlannerAgent + Orchestrator
- PlannerAgent analyzes instruction
- Creates step-by-step execution plan
- Orchestrator manages step execution order

### ✅ Requirement 3: Assign to Specialized Agents
**Implementation:** 4 Specialized Agents
- **PlannerAgent** - Task decomposition
- **RetrieverAgent** - Information gathering
- **AnalyzerAgent** - Data analysis
- **WriterAgent** - Output generation with LLM

### ✅ Requirement 4: Async Message Queues
**Implementation:** Redis-based queues
- Sorted sets for priority queuing
- Async enqueue/dequeue operations
- Pub/Sub for updates
- Retry logic and DLQ

### ✅ Requirement 5: Stream Partial Responses
**Implementation:** Server-Sent Events (SSE)
- `GET /stream/{task_id}` endpoint
- Real-time progress updates
- Step completion notifications
- Final output streaming

## Usage Examples

### Basic Task Submission

```powershell
# Submit task
$r = Invoke-RestMethod -Uri "http://localhost:8001/task" `
  -Method POST `
  -ContentType "application/json" `
  -Body '{"instruction": "Explain quantum computing"}'

# Get result
Start-Sleep 10
Invoke-RestMethod -Uri "http://localhost:8001/tasks/$($r.task_id)"
```

### Real-Time Streaming

```bash
# Using curl
curl -N http://localhost:8001/stream/task_abc123

# Output:
# event: connected
# data: {"task_id": "task_abc123"}
#
# event: progress
# data: {"status": "planning", "completed_steps": 0}
#
# event: progress
# data: {"status": "writing", "completed_steps": 2}
#
# event: complete
# data: {"status": "completed", "output": "..."}
```

### Health Check

```powershell
Invoke-RestMethod -Uri "http://localhost:8001/health"
```

## Development & Deployment

### Local Development

```powershell
# Install dependencies
pip install -r requirements.txt

# Set environment variables
$env:GROQ_API_KEY = "gsk_your_key"
$env:REDIS_HOST = "localhost"

# Start Redis
redis-server

# Start server
python main.py
```

### Production Deployment

1. **Environment Variables**
   - Set all required API keys
   - Configure Redis host/port
   - Set CORS origins appropriately

2. **Process Management**
   - Use supervisor/systemd for server
   - Run agents as separate services
   - Configure auto-restart on failure

3. **Redis**
   - Use Redis cluster for high availability
   - Configure persistence (RDB/AOF)
   - Set appropriate memory limits

4. **Monitoring**
   - Track queue depths
   - Monitor agent processing times
   - Alert on DLQ message accumulation
   - Track API latency and errors

### Testing

```powershell
# Verify all requirements
python verify_requirements.py

# Watch agent activity
python watch_agents.py

# Test specific task
python test_task.py
```

## Future Enhancements

### 1. Enhanced Agent Intelligence
- Integrate real LLM into PlannerAgent
- Add vector database for RetrieverAgent
- Specialized models for AnalyzerAgent

### 2. Advanced Features
- Task cancellation support
- Partial result streaming from WriterAgent
- Multi-modal support (images, audio)
- User authentication and rate limiting

### 3. Scalability
- Kubernetes deployment
- Agent auto-scaling based on queue depth
- Redis Cluster for distributed queuing

### 4. Observability
- Prometheus metrics
- Distributed tracing (OpenTelemetry)
- Centralized logging (ELK stack)
- Task execution DAG visualization

## Troubleshooting

### Task Stuck in Planning
- Check server logs for errors
- Verify Redis is running: `redis-cli ping`
- Restart agents: Stop server, clear Redis, restart

### No Output Generated
- Check if `GROQ_API_KEY` is set
- Verify API key is valid
- Check WriterAgent logs for API errors
- Fallback to mock mode if no key set

### SSE Stream Not Updating
- Check browser console for connection errors
- Verify task_id is correct
- Check server logs for orchestrator errors
- Try regular polling with `/tasks/{id}` instead

### Redis Connection Errors
- Verify Redis is running: `redis-cli ping`
- Check `REDIS_HOST` and `REDIS_PORT` settings
- Ensure firewall allows connection
- Check Redis logs for errors

## Architecture Diagrams

### Component Diagram
```
┌─────────────────────────────────────────────────┐
│                   User/Client                    │
└───────────────┬─────────────────────────────────┘
                │ HTTP/SSE
┌───────────────▼─────────────────────────────────┐
│              FastAPI Server                      │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐        │
│  │   POST   │ │   GET    │ │   SSE    │        │
│  │  /task   │ │ /tasks/  │ │ /stream/ │        │
│  └──────────┘ └──────────┘ └──────────┘        │
└───────────────┬─────────────────────────────────┘
                │
┌───────────────▼─────────────────────────────────┐
│          Task Orchestrator                       │
│  - Task decomposition                            │
│  - Agent coordination                            │
│  - State management                              │
└───┬───────────┬───────────┬───────────┬─────────┘
    │           │           │           │
    │ Redis Queues & Pub/Sub            │
    │           │           │           │
┌───▼────┐  ┌──▼─────┐ ┌──▼──────┐ ┌──▼──────┐
│Planner │  │Retriev.│ │Analyzer │ │ Writer  │
│ Agent  │→ │ Agent  │→│ Agent   │→│ Agent   │
└────────┘  └────────┘ └─────────┘ └────┬────┘
                                         │
                                    ┌────▼────┐
                                    │ Groq/   │
                                    │ OpenAI  │
                                    └─────────┘
```

### Message Flow Sequence
```
User → API: POST /task
API → Orchestrator: submit_task()
Orchestrator → Redis: store task state
Orchestrator → PlannerQueue: enqueue plan request

PlannerAgent → PlannerQueue: dequeue
PlannerAgent → PlannerAgent: create plan
PlannerAgent → Orchestrator: return plan

Orchestrator → RetrieverQueue: enqueue retrieve request
RetrieverAgent → RetrieverQueue: dequeue
RetrieverAgent → Orchestrator: return data

Orchestrator → AnalyzerQueue: enqueue analyze request
AnalyzerAgent → AnalyzerQueue: dequeue
AnalyzerAgent → Orchestrator: return analysis

Orchestrator → WriterQueue: enqueue write request
WriterAgent → WriterQueue: dequeue
WriterAgent → Groq: API call
Groq → WriterAgent: AI response
WriterAgent → Redis: store output
WriterAgent → Orchestrator: task complete

Orchestrator → Redis: update status
Orchestrator → PubSub: publish completion

User → API: GET /tasks/{id}
API → Redis: fetch result
Redis → API: return output
API → User: JSON response
```

## Conclusion

This agentic AI system provides a robust, scalable foundation for processing complex tasks through specialized agents. The architecture supports real-time streaming, fault tolerance, and easy integration with various LLM providers. The modular design allows for incremental enhancements and horizontal scaling as requirements grow.

---

**Document Version:** 1.0  
**Last Updated:** February 1, 2026  
**System Version:** 1.0.0
