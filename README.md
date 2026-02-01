# Agentic AI System

A production-grade, asynchronous multi-agent system for handling complex tasks through specialized AI agents coordinated via message queues.

## Overview

This system decomposes complex user tasks into structured execution plans and distributes work across specialized agents:

- **PlannerAgent**: Converts user instructions into step-by-step execution plans
- **RetrieverAgent**: Fetches relevant context and information
- **AnalyzerAgent**: Performs reasoning, validation, and data transformation
- **WriterAgent**: Generates user-facing output with streaming support

## Architecture

```
┌─────────────┐
│   FastAPI   │ ← HTTP/SSE Interface
│   Endpoint  │
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│  Orchestrator   │ ← Task coordination & dependency mgmt
└──────┬──────────┘
       │
       ▼
┌─────────────────┐
│  Redis Queues   │ ← Async message passing
└──────┬──────────┘
       │
       ▼
┌────────────────────────────────────┐
│  Planner → Retriever → Analyzer    │ ← Specialized agents
│            ↓                        │
│          Writer (streaming)         │
└────────────────────────────────────┘
```

## Features

✅ **Async Agent Orchestration**: Non-blocking task execution with dependency management  
✅ **Redis Message Queues**: Reliable inter-agent communication with priority support  
✅ **Server-Sent Events (SSE)**: Real-time progress streaming to clients  
✅ **Retry Logic**: Exponential backoff with configurable max attempts  
✅ **Dead-Letter Queue**: Failed message handling and debugging  
✅ **Stateless Agents**: Horizontally scalable worker pool design  
✅ **Graceful Degradation**: Partial failure recovery  
✅ **Production-Ready**: Comprehensive logging, error handling, and monitoring hooks

## Installation

### Prerequisites

- Python 3.10+
- Redis 6.0+

### Setup

1. **Clone and navigate to the project**:
```bash
cd agentic-ai-system
```

2. **Create virtual environment**:
```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate
```

3. **Install dependencies**:
```bash
pip install -r requirements.txt
```

4. **Configure environment**:
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. **Start Redis** (if not already running):
```bash
# Using Docker
docker run -d -p 6379:6379 redis:latest

# Or install locally and run
redis-server
```

## Usage

### Starting the System

```bash
python -m api.main
```

The API will be available at `http://localhost:8000`

### API Endpoints

#### Submit a Task

```bash
POST /task
Content-Type: application/json

{
  "instruction": "Analyze the benefits of async programming in distributed systems",
  "context": {}
}

Response:
{
  "task_id": "task_abc123def456",
  "message": "Task submitted successfully",
  "stream_url": "/stream/task_abc123def456"
}
```

#### Stream Progress (SSE)

```bash
GET /stream/{task_id}

# Server-Sent Events stream:
event: connected
data: {"task_id": "task_abc123def456"}

event: progress
data: {"task_id": "task_abc123def456", "status": "planning", "message": "Generating execution plan"}

event: progress
data: {"task_id": "task_abc123def456", "status": "retrieving", "message": "Retrieving information"}

event: chunk
data: {"chunk": "Based on the available information..."}

event: complete
data: {"task_id": "task_abc123def456", "status": "completed"}
```

#### Get Task Status

```bash
GET /status/{task_id}

Response:
{
  "task_id": "task_abc123def456",
  "status": "writing",
  "completed_steps": 2,
  "total_steps": 3
}
```

### Example with cURL

```bash
# Submit task
curl -X POST http://localhost:8000/task \
  -H "Content-Type: application/json" \
  -d '{"instruction": "Explain multi-agent systems"}'

# Stream progress (use the task_id from above)
curl -N http://localhost:8000/stream/task_abc123def456
```

### Example with Python

```python
import requests
import json

# Submit task
response = requests.post(
    "http://localhost:8000/task",
    json={"instruction": "Explain distributed computing patterns"}
)
task_data = response.json()
task_id = task_data["task_id"]

print(f"Task submitted: {task_id}")

# Stream progress
stream_url = f"http://localhost:8000/stream/{task_id}"
with requests.get(stream_url, stream=True) as r:
    for line in r.iter_lines():
        if line:
            print(line.decode())
```

## Configuration

Edit `.env` file to customize:

```env
# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Agent Timeouts (seconds)
PLANNER_TIMEOUT=30
RETRIEVER_TIMEOUT=20
ANALYZER_TIMEOUT=25
WRITER_TIMEOUT=40

# Queue Configuration
QUEUE_RETRY_MAX_ATTEMPTS=3
QUEUE_RETRY_BACKOFF_BASE=2

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
```

## Project Structure

```
agentic-ai-system/
├── agents/              # Specialized agent implementations
│   ├── base_agent.py    # Base agent with queue consumption
│   ├── planner_agent.py # Task planning & decomposition
│   ├── retriever_agent.py # Context retrieval
│   ├── analyzer_agent.py # Data analysis & reasoning
│   └── writer_agent.py  # Output generation with streaming
├── api/                 # FastAPI application
│   └── main.py          # API endpoints & SSE streaming
├── orchestrator/        # Task coordination
│   └── task_orchestrator.py # Dependency management & execution
├── queues/              # Redis queue infrastructure
│   └── redis_queue.py   # Queue client & retry logic
├── schemas/             # Pydantic data models
│   └── messages.py      # Message schemas for all agents
├── utils/               # Utilities
│   ├── logging_config.py
│   ├── monitoring.py
│   └── helpers.py
├── docs/                # Documentation
│   └── post_mortem.md   # Design decisions & trade-offs
├── config.py            # Configuration management
├── requirements.txt     # Python dependencies
└── README.md            # This file
```

## Monitoring & Observability

The system includes built-in logging and monitoring hooks:

- **Structured Logging**: All agents and components log with correlation IDs
- **Metrics Collection**: Track task duration, step execution times, errors
- **Health Checks**: `/health` endpoint for service monitoring
- **Dead-Letter Queue**: Failed messages for debugging

### Viewing Logs

Logs are written to stdout with the format:
```
2026-01-31 10:15:30 - PlannerAgent - INFO - Planning task: task_abc123
```

### Monitoring Redis Queues

```bash
# Connect to Redis CLI
redis-cli

# Check queue depths
ZCARD queue:planner
ZCARD queue:retriever
ZCARD queue:results

# View stream messages
XLEN stream:progress
```

## Scaling Considerations

### Horizontal Scaling

Each agent type can run multiple instances:

```bash
# Terminal 1: API + Orchestrator
python -m api.main

# Terminal 2: Additional Retriever workers
python -c "
import asyncio
from agents import RetrieverAgent
asyncio.run(RetrieverAgent().run())
"
```

### Performance Tuning

- Adjust agent timeouts in `.env`
- Increase Redis connection pool size
- Configure queue priority for critical tasks
- Implement rate limiting for external API calls

## Development

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-cov

# Run tests
pytest tests/ -v
```

### Code Quality

```bash
# Format code
black .

# Lint
flake8 .

# Type checking
mypy .
```

## Troubleshooting

### Redis Connection Issues

```bash
# Check Redis is running
redis-cli ping
# Should return: PONG

# Check Redis logs
redis-cli INFO
```

### Agent Not Processing Messages

1. Check agent is running: Look for startup logs
2. Verify queue has messages: `redis-cli ZCARD queue:planner`
3. Check for errors in logs
4. Ensure Redis connection is healthy

### Streaming Not Working

- Verify task was submitted successfully
- Check task_id is correct
- Ensure client supports SSE (Server-Sent Events)
- Check browser console for connection errors

## Production Deployment

### Docker Deployment

```dockerfile
# Dockerfile
FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "api.main"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
  
  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - REDIS_HOST=redis
    depends_on:
      - redis
```

### Environment Variables for Production

```env
# Production settings
REDIS_HOST=redis.production.local
REDIS_PASSWORD=secure_password
LLM_API_KEY=your_production_key
API_HOST=0.0.0.0
API_PORT=8000
```

## Security Considerations

- **API Keys**: Never commit `.env` file with real credentials
- **CORS**: Configure `CORS_ORIGINS` for production domains only
- **Rate Limiting**: Implement request rate limiting (e.g., slowapi)
- **Input Validation**: All inputs validated via Pydantic models
- **Redis Authentication**: Use Redis password in production

## Performance Benchmarks

Typical latency (mock LLM, local Redis):

- Task submission: ~10ms
- Planning phase: ~100ms
- Retrieval step: ~500ms
- Analysis step: ~700ms
- Writing (streaming): ~2-3s
- **Total end-to-end**: ~4-5s

## License

MIT License - See LICENSE file for details

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Support

For issues and questions:
- GitHub Issues: [Create an issue]
- Documentation: See `/docs` directory

## Roadmap

- [ ] LLM integration (OpenAI, Anthropic, Together)
- [ ] Vector database support (Pinecone, Weaviate)
- [ ] Prometheus metrics exporter
- [ ] GraphQL API option
- [ ] Agent result caching
- [ ] Multi-tenancy support
- [ ] Web UI dashboard
