"""
Simple script to run all agents for development/testing.
"""
import asyncio
import logging
from agents import PlannerAgent, RetrieverAgent, AnalyzerAgent, WriterAgent
from utils import setup_logging


async def main():
    """Run all agents."""
    setup_logging(level="INFO")
    
    logger = logging.getLogger(__name__)
    logger.info("Starting all agents...")
    
    # Create agents
    planner = PlannerAgent()
    retriever = RetrieverAgent()
    analyzer = AnalyzerAgent()
    writer = WriterAgent()
    
    # Run all agents concurrently
    try:
        await asyncio.gather(
            planner.run(),
            retriever.run(),
            analyzer.run(),
            writer.run(),
        )
    except KeyboardInterrupt:
        logger.info("Shutting down agents...")
        planner.stop()
        retriever.stop()
        analyzer.stop()
        writer.stop()


if __name__ == "__main__":
    asyncio.run(main())
