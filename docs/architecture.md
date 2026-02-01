# Architecture Documentation

## System Components

### 1. API Layer (`api/`)

**FastAPI Application** - HTTP interface with SSE streaming

- **Endpoints**:
  - `POST /task` - Submit new task
  - `GET /stream/{task_id}` - Stream progress via SSE
  - `GET /status/{task_id}` - Get task status
  - `GET /health` - Health check

- **Lifespan Management**: 
  - Starts agents and orchestrator on startup
  - Graceful shutdown on SIGTERM

### 2. Orchestrator (`orchestrator/`)

**TaskOrchestrator** - Central coordination engine

**Responsibilities**:
- Receive task requests
- Coordinate PlannerAgent to generate execution plan
- Execute steps based on dependency graph
- Track progress and publish updates
- Aggregate final results
- Handle failures and retries

**State Management**:
```python
{
    "task_id": {
        "status": TaskStatus,
        "plan": List[ExecutionStep],
        "completed_steps": Set[str],
        "step_results": Dict[str, Any],
        "start_time": datetime
    }
}
```

### 3. Agents (`agents/`)

#### BaseAgent
- Abstract base class
- Queue consumption loop
- Timeout handling
- Retry logic with exponential backoff
- DLQ routing

#### PlannerAgent
- Converts user instruction → execution plan
- Determines agent assignments
- Identifies step dependencies
- Current: Rule-based planning
- Future: LLM-powered planning

#### RetrieverAgent
- Fetches relevant context
- Current: Mock knowledge base
- Future: Vector DB, search APIs, RAG

#### AnalyzerAgent
- Data validation
- Pattern recognition
- Insight extraction
- Comparative analysis

#### WriterAgent
- Generates final output
- Streaming support via chunks
- Current: Mock generation
- Future: LLM integration (OpenAI, Anthropic, Together)

### 4. Queue System (`queues/`)

**RedisQueueClient** - Async message queue operations

**Queue Types**:
- **Sorted Sets**: Priority queues for agents
  - `queue:planner`
  - `queue:retriever`
  - `queue:analyzer`
  - `queue:writer`
  - `queue:results`
  - `queue:dlq` (dead-letter)

- **Streams**: Broadcast progress
  - `stream:progress`

**Features**:
- Priority-based message ordering
- Exponential backoff retry
- Dead-letter queue for failures
- TTL for message expiration

### 5. Schemas (`schemas/`)

**Pydantic Models** - Type-safe message definitions

- `TaskRequest` - Initial task submission
- `PlanMessage` / `PlanResult` - Planning phase
- `RetrieveMessage` / `RetrievalResult` - Retrieval
- `AnalyzeMessage` / `AnalysisResult` - Analysis
- `WriteMessage` / `WriteResult` - Writing
- `ProgressUpdate` - Status updates
- `TaskResult` - Final output

### 6. Utilities (`utils/`)

- **logging_config.py**: Structured logging setup
- **monitoring.py**: Metrics collection
- **helpers.py**: Common utilities

---

## Message Flow

```
User Request
    ↓
[POST /task] → TaskRequest
    ↓
Orchestrator → PlanMessage → [queue:planner]
    ↓
PlannerAgent processes → PlanResult → [queue:results]
    ↓
Orchestrator receives plan
    ↓
For each step (with dependencies):
    ├─ RetrieveMessage → [queue:retriever] → RetrieverAgent
    ├─ AnalyzeMessage → [queue:analyzer] → AnalyzerAgent  
    └─ WriteMessage → [queue:writer] → WriterAgent
         ↓
    Results → [queue:results] → Orchestrator
    ↓
Progress published → [stream:progress]
    ↓
[GET /stream/{task_id}] ← Client receives SSE
    ↓
Task Complete → TaskResult
```

---

## Dependency Management

Orchestrator ensures steps execute in correct order:

```python
Step 1: RETRIEVE (no dependencies)
Step 2: ANALYZE (depends_on: [step_1])
Step 3: WRITE (depends_on: [step_1, step_2])
```

Execution flow:
1. Execute Step 1 (independent)
2. Wait for Step 1 completion
3. Execute Step 2 (dependency satisfied)
4. Wait for Steps 1 & 2
5. Execute Step 3 (all dependencies satisfied)

---

## Error Handling

### Retry Strategy

```python
Attempt 1: Immediate
Attempt 2: Wait 2^0 = 1s
Attempt 3: Wait 2^1 = 2s
Attempt 4: Wait 2^2 = 4s
Failed → Move to DLQ
```

### Failure Modes

| Failure Type | Handling |
|--------------|----------|
| Agent timeout | Retry with backoff |
| Message parsing error | Move to DLQ |
| Redis connection lost | Reconnect automatically |
| Step failure | Mark task as failed |
| Client disconnect | Continue processing |

---

## Scalability

### Current Limits
- **Orchestrator**: 1,000 concurrent tasks (in-memory state)
- **Redis**: ~10,000 queued messages before memory pressure
- **Agents**: CPU-bound, can run multiple instances
- **SSE**: Limited by open connections (~10,000/server)

### Scaling Strategies

**Horizontal Agent Scaling**:
```bash
# Multiple retriever instances
docker-compose scale retriever=5
```

**Vertical Orchestrator Scaling**:
- Move state to Redis
- Use distributed locks
- Multiple orchestrator instances

**Sharding**:
- Partition tasks by user ID
- Separate Redis instances per shard
- Route requests via consistent hashing

---

## Configuration

All settings in `.env`:

```env
# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# Timeouts (seconds)
PLANNER_TIMEOUT=30
RETRIEVER_TIMEOUT=20
ANALYZER_TIMEOUT=25
WRITER_TIMEOUT=40

# Retry
QUEUE_RETRY_MAX_ATTEMPTS=3
QUEUE_RETRY_BACKOFF_BASE=2
```

---

## Monitoring Hooks

### Logs
- Structured JSON logs
- Correlation IDs for tracing
- Log levels: DEBUG, INFO, WARNING, ERROR

### Metrics (to implement)
- Task duration histogram
- Queue depth gauge
- Error rate counter
- Agent utilization

### Health Checks
```bash
GET /health
{
  "status": "healthy",
  "components": {
    "redis": "healthy",
    "orchestrator": "running"
  }
}
```

---

## Security

### Current
- Input validation via Pydantic
- CORS configuration
- No authentication (development)

### Production Requirements
- API key authentication
- Rate limiting (per user)
- Input sanitization
- Redis password auth
- TLS/SSL for Redis
- Request signing

---

## Testing Strategy

### Unit Tests
- Individual agent logic
- Queue operations
- Schema validation

### Integration Tests
- End-to-end task flow
- Agent coordination
- Error handling

### Load Tests
- Concurrent task submissions
- Queue throughput
- Memory leak detection

---

## Deployment

### Docker Compose
```yaml
services:
  redis:
    image: redis:7-alpine
  
  orchestrator:
    build: .
    environment:
      - REDIS_HOST=redis
    depends_on:
      - redis
  
  retriever:
    build: .
    command: python run_agents.py
```

### Kubernetes
- Deployment for API + Orchestrator
- StatefulSet for Redis
- Horizontal Pod Autoscaler for agents
- ConfigMap for environment variables
- Secret for sensitive data

---

## Future Enhancements

1. **LLM Integration**: Real API calls with streaming
2. **Vector Database**: Semantic search for retrieval
3. **Distributed Tracing**: OpenTelemetry spans
4. **GraphQL API**: Alternative to REST
5. **Result Caching**: Avoid duplicate work
6. **Agent Marketplace**: Pluggable agent ecosystem
