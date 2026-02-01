"""
Orchestrator - Coordinates agent execution based on the plan.
Manages dependencies, tracks progress, and handles failures.
"""
import asyncio
import logging
from typing import Dict, Any, List, Set, Optional
from datetime import datetime

from schemas import (
    TaskRequest, ExecutionStep, PlanMessage, PlanResult,
    RetrieveMessage, AnalyzeMessage, WriteMessage,
    TaskStatus, ProgressUpdate, TaskResult, AgentType, StepType
)
from queues import queue_client, QueueConfig
from config import settings

logger = logging.getLogger(__name__)


class TaskOrchestrator:
    """
    Orchestrates multi-agent task execution.
    
    Responsibilities:
    - Receive task requests
    - Send planning request to PlannerAgent
    - Execute steps based on dependencies
    - Track progress and publish updates
    - Handle partial failures with retry logic
    - Aggregate final results
    """
    
    def __init__(self):
        self.active_tasks: Dict[str, Dict[str, Any]] = {}
        self._running = False
    
    async def start(self) -> None:
        """Start the orchestrator main loop."""
        self._running = True
        logger.info("Orchestrator started")
        
        # Start result processor
        asyncio.create_task(self._process_results())
    
    async def submit_task(self, task: TaskRequest) -> bool:
        """
        Submit a new task for processing.
        
        Args:
            task: Task request
            
        Returns:
            True if submitted successfully
        """
        task_id = task.task_id
        
        logger.info(f"Submitting task: {task_id}")
        
        # Initialize task state
        self.active_tasks[task_id] = {
            "status": TaskStatus.PENDING,
            "request": task,
            "plan": None,
            "completed_steps": set(),
            "step_results": {},
            "start_time": datetime.utcnow(),
            "current_step": None
        }
        
        # Publish initial progress
        await self._publish_progress(
            task_id,
            TaskStatus.PENDING,
            "Task received and queued"
        )
        
        # Send to planner
        plan_msg = PlanMessage(
            correlation_id=task_id,
            user_instruction=task.user_instruction,
            context=task.context
        )
        
        await queue_client.enqueue(QueueConfig.PLANNER_QUEUE, plan_msg)
        
        # Update status
        self.active_tasks[task_id]["status"] = TaskStatus.PLANNING
        await self._publish_progress(
            task_id,
            TaskStatus.PLANNING,
            "Generating execution plan"
        )
        
        return True
    
    async def _process_results(self) -> None:
        """
        Process results from agents.
        Main orchestration loop that handles plan results and step completions.
        """
        logger.info("Result processor started")
        
        while self._running:
            try:
                # Poll for results
                result = await queue_client.dequeue(QueueConfig.RESULTS_QUEUE, timeout=1)
                
                if result is None:
                    await asyncio.sleep(0.1)
                    continue
                
                correlation_id = result.get("correlation_id")
                
                if correlation_id not in self.active_tasks:
                    logger.warning(f"Received result for unknown task: {correlation_id}")
                    continue
                
                # Route result based on type
                if "steps" in result:  # PlanResult
                    await self._handle_plan_result(correlation_id, result)
                elif "documents" in result:  # RetrievalResult
                    await self._handle_step_result(correlation_id, result)
                elif "insights" in result:  # AnalysisResult
                    await self._handle_step_result(correlation_id, result)
                elif "content" in result:  # WriteResult
                    await self._handle_step_result(correlation_id, result)
                
            except Exception as e:
                logger.error(f"Error processing result: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    async def _handle_plan_result(
        self,
        task_id: str,
        result: Dict[str, Any]
    ) -> None:
        """
        Handle plan result from PlannerAgent.
        
        Args:
            task_id: Task ID
            result: PlanResult dictionary
        """
        logger.info(f"Received plan for task: {task_id}")
        
        if not result.get("success"):
            # Planning failed
            await self._fail_task(task_id, result.get("error", "Planning failed"))
            return
        
        # Store plan
        steps = [ExecutionStep(**step) for step in result["steps"]]
        self.active_tasks[task_id]["plan"] = steps
        self.active_tasks[task_id]["status"] = TaskStatus.RETRIEVING
        
        await self._publish_progress(
            task_id,
            TaskStatus.RETRIEVING,
            f"Plan created with {len(steps)} steps. Starting execution."
        )
        
        # Start executing steps
        await self._execute_next_steps(task_id)
    
    async def _handle_step_result(
        self,
        task_id: str,
        result: Dict[str, Any]
    ) -> None:
        """
        Handle step completion result.
        
        Args:
            task_id: Task ID
            result: Step result dictionary
        """
        step_id = result.get("step_id")
        success = result.get("success", True)
        
        logger.info(f"Step {step_id} completed for task {task_id}: {success}")
        
        if not success:
            # Step failed - check if critical
            error = result.get("error", "Step execution failed")
            await self._handle_step_failure(task_id, step_id, error)
            return
        
        # Mark step as completed
        task_state = self.active_tasks[task_id]
        task_state["completed_steps"].add(step_id)
        task_state["step_results"][step_id] = result
        
        # Update progress
        plan = task_state["plan"]
        progress = int((len(task_state["completed_steps"]) / len(plan)) * 100)
        
        await self._publish_progress(
            task_id,
            self._get_current_status(task_id),
            f"Completed step {step_id}",
            step_id=step_id,
            progress_percent=progress
        )
        
        # Check if task is complete
        if len(task_state["completed_steps"]) == len(plan):
            await self._complete_task(task_id)
        else:
            # Execute next steps
            await self._execute_next_steps(task_id)
    
    async def _execute_next_steps(self, task_id: str) -> None:
        """
        Execute steps that have their dependencies satisfied.
        
        Args:
            task_id: Task ID
        """
        task_state = self.active_tasks[task_id]
        plan = task_state["plan"]
        completed = task_state["completed_steps"]
        
        for step in plan:
            # Skip if already completed
            if step.step_id in completed:
                continue
            
            # Check dependencies
            if step.depends_on:
                if not all(dep in completed for dep in step.depends_on):
                    continue  # Dependencies not satisfied
            
            # Execute step
            await self._execute_step(task_id, step)
    
    async def _execute_step(
        self,
        task_id: str,
        step: ExecutionStep
    ) -> None:
        """
        Execute a single step by routing to appropriate agent.
        
        Args:
            task_id: Task ID
            step: Execution step
        """
        logger.info(f"Executing step {step.step_id} for task {task_id}")
        
        task_state = self.active_tasks[task_id]
        task_state["current_step"] = step.step_id
        
        # Update status based on step type
        if step.step_type == StepType.RETRIEVE:
            task_state["status"] = TaskStatus.RETRIEVING
            await self._execute_retrieve_step(task_id, step)
            
        elif step.step_type == StepType.ANALYZE:
            task_state["status"] = TaskStatus.ANALYZING
            await self._execute_analyze_step(task_id, step)
            
        elif step.step_type == StepType.WRITE:
            task_state["status"] = TaskStatus.WRITING
            await self._execute_write_step(task_id, step)
    
    async def _execute_retrieve_step(
        self,
        task_id: str,
        step: ExecutionStep
    ) -> None:
        """Execute retrieval step."""
        params = step.parameters
        
        message = RetrieveMessage(
            correlation_id=task_id,
            step_id=step.step_id,
            query=params.get("query", ""),
            max_results=params.get("max_results", 5)
        )
        
        await queue_client.enqueue(QueueConfig.RETRIEVER_QUEUE, message)
        
        await self._publish_progress(
            task_id,
            TaskStatus.RETRIEVING,
            f"Retrieving information: {step.description}",
            step_id=step.step_id
        )
    
    async def _execute_analyze_step(
        self,
        task_id: str,
        step: ExecutionStep
    ) -> None:
        """Execute analysis step."""
        task_state = self.active_tasks[task_id]
        params = step.parameters
        
        # Gather data from dependencies
        data = {}
        if step.depends_on:
            for dep_id in step.depends_on:
                if dep_id in task_state["step_results"]:
                    data[dep_id] = task_state["step_results"][dep_id]
        
        message = AnalyzeMessage(
            correlation_id=task_id,
            step_id=step.step_id,
            data=data,
            analysis_type=params.get("analysis_type", "general"),
            parameters=params
        )
        
        await queue_client.enqueue(QueueConfig.ANALYZER_QUEUE, message)
        
        await self._publish_progress(
            task_id,
            TaskStatus.ANALYZING,
            f"Analyzing data: {step.description}",
            step_id=step.step_id
        )
    
    async def _execute_write_step(
        self,
        task_id: str,
        step: ExecutionStep
    ) -> None:
        """Execute writing step."""
        task_state = self.active_tasks[task_id]
        params = step.parameters
        
        # Gather all previous results as context
        context = {}
        for step_id, result in task_state["step_results"].items():
            context[step_id] = result
        
        message = WriteMessage(
            correlation_id=task_id,
            step_id=step.step_id,
            prompt=params.get("instruction", task_state["request"].user_instruction),
            context=context,
            stream=True
        )
        
        await queue_client.enqueue(QueueConfig.WRITER_QUEUE, message)
        
        await self._publish_progress(
            task_id,
            TaskStatus.WRITING,
            f"Generating response: {step.description}",
            step_id=step.step_id
        )
    
    async def _handle_step_failure(
        self,
        task_id: str,
        step_id: str,
        error: str
    ) -> None:
        """
        Handle step failure with graceful degradation.
        
        Args:
            task_id: Task ID
            step_id: Failed step ID
            error: Error message
        """
        logger.error(f"Step {step_id} failed for task {task_id}: {error}")
        
        # For now, fail the entire task
        # In production, implement partial failure recovery
        await self._fail_task(task_id, f"Step {step_id} failed: {error}")
    
    async def _complete_task(self, task_id: str) -> None:
        """
        Mark task as completed and publish final result.
        
        Args:
            task_id: Task ID
        """
        task_state = self.active_tasks[task_id]
        
        # Find final output (from writer)
        final_output = None
        for result in task_state["step_results"].values():
            if "content" in result:
                final_output = result["content"]
                break
        
        # Calculate duration
        duration = (datetime.utcnow() - task_state["start_time"]).total_seconds()
        
        result = TaskResult(
            task_id=task_id,
            status=TaskStatus.COMPLETED,
            output=final_output,
            steps_completed=list(task_state["completed_steps"]),
            duration=duration
        )
        
        # Store final result
        await queue_client.set_with_ttl(
            f"task:result:{task_id}",
            result.model_dump(),
            ttl=7200  # 2 hours
        )
        
        await self._publish_progress(
            task_id,
            TaskStatus.COMPLETED,
            "Task completed successfully",
            progress_percent=100
        )
        
        logger.info(f"Task {task_id} completed in {duration:.2f}s")
        
        # Clean up after delay
        await asyncio.sleep(300)  # Keep for 5 minutes
        if task_id in self.active_tasks:
            del self.active_tasks[task_id]
    
    async def _fail_task(self, task_id: str, error: str) -> None:
        """
        Mark task as failed.
        
        Args:
            task_id: Task ID
            error: Error message
        """
        task_state = self.active_tasks.get(task_id)
        if not task_state:
            return
        
        duration = (datetime.utcnow() - task_state["start_time"]).total_seconds()
        
        result = TaskResult(
            task_id=task_id,
            status=TaskStatus.FAILED,
            error=error,
            steps_completed=list(task_state["completed_steps"]),
            duration=duration
        )
        
        # Store final result
        await queue_client.set_with_ttl(
            f"task:result:{task_id}",
            result.model_dump(),
            ttl=7200
        )
        
        await self._publish_progress(
            task_id,
            TaskStatus.FAILED,
            f"Task failed: {error}"
        )
        
        logger.error(f"Task {task_id} failed: {error}")
    
    async def _publish_progress(
        self,
        task_id: str,
        status: TaskStatus,
        message: str,
        step_id: Optional[str] = None,
        progress_percent: Optional[int] = None
    ) -> None:
        """
        Publish progress update to stream.
        
        Args:
            task_id: Task ID
            status: Current status
            message: Status message
            step_id: Current step ID
            progress_percent: Progress percentage
        """
        update = ProgressUpdate(
            task_id=task_id,
            status=status,
            message=message,
            step_id=step_id,
            progress_percent=progress_percent
        )
        
        await queue_client.publish_stream(
            QueueConfig.PROGRESS_STREAM,
            {
                "task_id": task_id,
                "type": "progress",
                "data": update.model_dump_json()
            }
        )
    
    def _get_current_status(self, task_id: str) -> TaskStatus:
        """Get current task status."""
        return self.active_tasks[task_id]["status"]
    
    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current status of a task including output when available.
        
        Args:
            task_id: Task ID
            
        Returns:
            Task state or None if not found
        """
        if task_id in self.active_tasks:
            state = self.active_tasks[task_id]
            
            # Get output if writer has completed
            output = None
            for step_id, result in state.get("step_results", {}).items():
                if "content" in result:
                    output = result["content"]
                    break
            
            return {
                "task_id": task_id,
                "status": state["status"].value,
                "completed_steps": len(state["completed_steps"]),
                "total_steps": len(state["plan"]) if state["plan"] else 0,
                "output": output
            }
        
        # Check if completed task exists in Redis
        result = await queue_client.get(f"task:result:{task_id}")
        return result
    
    def stop(self) -> None:
        """Stop the orchestrator."""
        logger.info("Stopping orchestrator")
        self._running = False


# Global orchestrator instance
orchestrator = TaskOrchestrator()
