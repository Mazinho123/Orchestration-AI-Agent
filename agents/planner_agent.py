"""
Planner Agent - Decomposes user tasks into structured execution plans.
"""
import logging
from typing import Dict, Any, List
import json

from agents.base_agent import BaseAgent
from schemas import (
    PlanMessage, PlanResult, ExecutionStep, 
    AgentType, StepType
)
from queues import queue_client, QueueConfig
from config import settings

logger = logging.getLogger(__name__)


class PlannerAgent(BaseAgent):
    """
    Converts complex user instructions into structured step-by-step plans.
    Determines which agent should handle each step and their dependencies.
    """
    
    def __init__(self):
        super().__init__(
            name="PlannerAgent",
            input_queue=QueueConfig.PLANNER_QUEUE,
            timeout=settings.planner_timeout
        )
    
    async def process(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate an execution plan from user instruction.
        
        Args:
            message: PlanMessage dictionary
            
        Returns:
            PlanResult dictionary
        """
        correlation_id = message["correlation_id"]
        user_instruction = message["user_instruction"]
        
        logger.info(f"Planning task: {correlation_id}")
        
        try:
            # Generate plan using LLM or rule-based logic
            steps = await self._generate_plan(user_instruction)
            
            result = PlanResult(
                correlation_id=correlation_id,
                steps=steps,
                estimated_duration=len(steps) * 15,  # Rough estimate
                success=True
            )
            
            # Publish result to results queue for orchestrator
            await queue_client.enqueue(
                QueueConfig.RESULTS_QUEUE,
                result
            )
            
            return result.model_dump()
            
        except Exception as e:
            logger.error(f"Planning failed: {e}", exc_info=True)
            
            error_result = PlanResult(
                correlation_id=correlation_id,
                steps=[],
                success=False,
                error=str(e)
            )
            
            await queue_client.enqueue(
                QueueConfig.RESULTS_QUEUE,
                error_result
            )
            
            raise
    
    async def _generate_plan(self, instruction: str) -> List[ExecutionStep]:
        """
        Generate execution steps from user instruction.
        
        This is a simplified implementation. In production, this would:
        1. Use an LLM to intelligently parse the instruction
        2. Identify required information retrieval
        3. Determine analysis needs
        4. Structure the output format
        
        Args:
            instruction: User's task description
            
        Returns:
            List of execution steps
        """
        # Simple rule-based planning for demonstration
        # In production, replace with LLM-based planning
        
        steps = []
        
        # Step 1: Always retrieve relevant context
        steps.append(ExecutionStep(
            step_id="step_1",
            step_type=StepType.RETRIEVE,
            agent=AgentType.RETRIEVER,
            description="Retrieve relevant context and information",
            parameters={
                "query": self._extract_key_terms(instruction),
                "max_results": 5
            }
        ))
        
        # Step 2: Analyze if instruction seems to require reasoning
        if any(keyword in instruction.lower() for keyword in 
               ["analyze", "compare", "evaluate", "assess", "determine"]):
            steps.append(ExecutionStep(
                step_id="step_2",
                step_type=StepType.ANALYZE,
                agent=AgentType.ANALYZER,
                description="Analyze retrieved information",
                depends_on=["step_1"],
                parameters={
                    "analysis_type": "general",
                    "instruction": instruction
                }
            ))
            writer_depends = ["step_1", "step_2"]
        else:
            writer_depends = ["step_1"]
        
        # Step 3: Generate final output
        steps.append(ExecutionStep(
            step_id="step_3",
            step_type=StepType.WRITE,
            agent=AgentType.WRITER,
            description="Generate comprehensive response",
            depends_on=writer_depends,
            parameters={
                "instruction": instruction,
                "format": "detailed"
            }
        ))
        
        logger.info(f"Generated plan with {len(steps)} steps")
        return steps
    
    def _extract_key_terms(self, text: str) -> str:
        """
        Extract key terms from instruction for retrieval.
        Simple implementation - in production use NLP techniques.
        
        Args:
            text: Input text
            
        Returns:
            Key terms string
        """
        # Remove common stop words and extract meaningful terms
        stop_words = {"the", "a", "an", "and", "or", "but", "is", "are", "was", "were",
                     "in", "on", "at", "to", "for", "of", "with", "by"}
        
        words = text.lower().split()
        key_terms = [w for w in words if w not in stop_words and len(w) > 3]
        
        return " ".join(key_terms[:10])  # Top 10 terms
