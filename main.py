#!/usr/bin/env python
"""
Main entry point for the Agentic AI System.
Run with: python main.py
"""
import sys
import asyncio
import uvicorn

if __name__ == "__main__":
    # Run the FastAPI app
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8001,
        reload=False,
        log_level="info"
    )
