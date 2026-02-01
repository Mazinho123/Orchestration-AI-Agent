"""
Analyzer Agent - Performs reasoning and data transformation.
"""
import logging
import asyncio
from typing import Dict, Any
import json

from agents.base_agent import BaseAgent
from schemas import AnalyzeMessage, AnalysisResult
from queues import queue_client, QueueConfig
from config import settings

logger = logging.getLogger(__name__)


class AnalyzerAgent(BaseAgent):
    """
    Performs analysis, reasoning, and transformation on retrieved data.
    
    Capabilities:
    - Data validation and verification
    - Pattern recognition
    - Comparative analysis
    - Insight extraction
    - Structured data transformation
    """
    
    def __init__(self):
        super().__init__(
            name="AnalyzerAgent",
            input_queue=QueueConfig.ANALYZER_QUEUE,
            timeout=settings.analyzer_timeout
        )
    
    async def process(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze data and generate insights.
        
        Args:
            message: AnalyzeMessage dictionary
            
        Returns:
            AnalysisResult dictionary
        """
        correlation_id = message["correlation_id"]
        step_id = message["step_id"]
        data = message["data"]
        analysis_type = message["analysis_type"]
        parameters = message.get("parameters", {})
        
        logger.info(f"Analyzing data for task: {correlation_id}")
        
        try:
            # Perform analysis based on type
            insights = await self._analyze(data, analysis_type, parameters)
            
            result = AnalysisResult(
                correlation_id=correlation_id,
                step_id=step_id,
                insights=insights,
                confidence=0.85,  # Mock confidence score
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
            logger.error(f"Analysis failed: {e}", exc_info=True)
            
            error_result = AnalysisResult(
                correlation_id=correlation_id,
                step_id=step_id,
                insights={},
                success=False,
                error=str(e)
            )
            
            await queue_client.enqueue(
                QueueConfig.RESULTS_QUEUE,
                error_result
            )
            
            raise
    
    async def _analyze(
        self,
        data: Dict[str, Any],
        analysis_type: str,
        parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Perform analysis on data.
        
        In production, this would:
        1. Use ML models for classification/regression
        2. Apply business logic and rules
        3. Perform statistical analysis
        4. Extract structured insights
        
        Args:
            data: Input data to analyze
            analysis_type: Type of analysis to perform
            parameters: Additional parameters
            
        Returns:
            Analysis insights dictionary
        """
        # Simulate async processing
        await asyncio.sleep(0.7)
        
        insights = {
            "analysis_type": analysis_type,
            "processed_at": asyncio.get_event_loop().time(),
            "summary": {},
            "findings": []
        }
        
        # Perform different analyses based on type
        if analysis_type == "general":
            insights["summary"] = self._general_analysis(data)
            
        elif analysis_type == "comparative":
            insights["summary"] = self._comparative_analysis(data)
            
        elif analysis_type == "validation":
            insights["summary"] = self._validation_analysis(data)
        
        else:
            # Default analysis
            insights["summary"] = {
                "data_keys": list(data.keys()),
                "data_types": {k: type(v).__name__ for k, v in data.items()},
                "complexity": "moderate"
            }
        
        # Extract findings
        insights["findings"] = self._extract_findings(data, parameters)
        
        return insights
    
    def _general_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Perform general analysis on data."""
        return {
            "total_items": len(data.get("documents", [])) if "documents" in data else 0,
            "has_content": bool(data),
            "data_quality": "good" if data else "poor",
            "key_themes": self._extract_themes(data)
        }
    
    def _comparative_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Compare multiple data points."""
        documents = data.get("documents", [])
        
        return {
            "comparison_count": len(documents),
            "similarities": ["All sources discuss AI systems"],
            "differences": ["Different focus areas and depth"],
            "recommendation": "Synthesize multiple perspectives"
        }
    
    def _validation_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data quality and completeness."""
        return {
            "is_valid": True,
            "completeness_score": 0.9,
            "issues_found": [],
            "quality_rating": "high"
        }
    
    def _extract_themes(self, data: Dict[str, Any]) -> list:
        """Extract key themes from data."""
        themes = []
        
        documents = data.get("documents", [])
        for doc in documents:
            if "metadata" in doc and "category" in doc["metadata"]:
                themes.append(doc["metadata"]["category"])
        
        return list(set(themes))
    
    def _extract_findings(
        self,
        data: Dict[str, Any],
        parameters: Dict[str, Any]
    ) -> list:
        """Extract specific findings based on analysis."""
        findings = []
        
        # Extract instruction-specific findings
        instruction = parameters.get("instruction", "")
        
        if "analyze" in instruction.lower():
            findings.append({
                "type": "analysis_required",
                "description": "Detailed analysis requested by user",
                "priority": "high"
            })
        
        # Check data completeness
        if data.get("documents"):
            findings.append({
                "type": "data_available",
                "description": f"Retrieved {len(data['documents'])} relevant documents",
                "priority": "medium"
            })
        
        return findings
