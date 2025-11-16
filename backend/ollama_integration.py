# ollama_integration.py - Core Ollama Integration for Universal Data Understanding

"""
Ollama Integration for Dynamic ETL Pipeline

This module provides integration with Ollama to enable:
1. Universal data type detection (beyond predefined categories)
2. Dynamic category learning
3. Semantic understanding of any data domain
4. Natural language query processing
5. Intelligent transformation suggestions
"""

import json
import logging
import requests
from typing import Dict, List, Optional, Any, Tuple
import time
from datetime import datetime

logger = logging.getLogger(__name__)


class OllamaClient:
    """
    Client for interacting with Ollama LLM for data understanding

    Supports multiple models:
    - llama2: General purpose
    - codellama: Code and data structures
    - mistral: Fast inference
    - neural-chat: Conversational
    """

    def __init__(
        self,
        base_url: str = "http://localhost:11434",
        model: str = "llama2",
        timeout: int = 120
    ):
        """
        Initialize Ollama client

        Args:
            base_url: Ollama API endpoint
            model: Model to use (llama2, mistral, codellama, etc.)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.model = model
        self.timeout = timeout

        # Check if Ollama is running
        if not self._check_connection():
            logger.warning(
                f"Could not connect to Ollama at {base_url}. "
                f"Make sure Ollama is running: 'ollama serve'"
            )
        else:
            logger.info(f"Connected to Ollama using model: {model}")

    def _check_connection(self) -> bool:
        """Check if Ollama is accessible"""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"Ollama connection check failed: {e}")
            return False

    def list_models(self) -> List[str]:
        """List available models"""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=10)
            if response.status_code == 200:
                models = response.json().get('models', [])
                return [m['name'] for m in models]
            return []
        except Exception as e:
            logger.error(f"Failed to list models: {e}")
            return []

    def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        stream: bool = False
    ) -> Dict[str, Any]:
        """
        Generate completion using Ollama

        Args:
            prompt: User prompt
            system_prompt: System prompt for context
            temperature: Sampling temperature (0.0 - 2.0)
            max_tokens: Maximum tokens to generate
            stream: Whether to stream response

        Returns:
            Response dictionary with 'response' key
        """
        try:
            payload = {
                "model": self.model,
                "prompt": prompt,
                "stream": stream,
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens,
                }
            }

            if system_prompt:
                payload["system"] = system_prompt

            response = requests.post(
                f"{self.base_url}/api/generate",
                json=payload,
                timeout=self.timeout,
                stream=stream
            )

            if stream:
                # Handle streaming response
                full_response = ""
                for line in response.iter_lines():
                    if line:
                        chunk = json.loads(line)
                        if 'response' in chunk:
                            full_response += chunk['response']
                return {"response": full_response}
            else:
                # Handle non-streaming response
                result = response.json()
                return result

        except requests.exceptions.Timeout:
            logger.error(f"Ollama request timed out after {self.timeout}s")
            return {"response": "", "error": "timeout"}
        except Exception as e:
            logger.error(f"Ollama generation failed: {e}")
            return {"response": "", "error": str(e)}

    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 2000
    ) -> Dict[str, Any]:
        """
        Chat completion with conversation history

        Args:
            messages: List of {'role': 'user/assistant', 'content': '...'}
            temperature: Sampling temperature
            max_tokens: Maximum tokens

        Returns:
            Response dictionary
        """
        try:
            payload = {
                "model": self.model,
                "messages": messages,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens,
                }
            }

            response = requests.post(
                f"{self.base_url}/api/chat",
                json=payload,
                timeout=self.timeout
            )

            return response.json()

        except Exception as e:
            logger.error(f"Ollama chat failed: {e}")
            return {"message": {"content": ""}, "error": str(e)}

    def embed(self, text: str) -> List[float]:
        """
        Generate embeddings for text

        Args:
            text: Text to embed

        Returns:
            List of embedding values
        """
        try:
            payload = {
                "model": self.model,
                "prompt": text
            }

            response = requests.post(
                f"{self.base_url}/api/embeddings",
                json=payload,
                timeout=30
            )

            result = response.json()
            return result.get('embedding', [])

        except Exception as e:
            logger.error(f"Ollama embedding failed: {e}")
            return []

    def pull_model(self, model_name: str) -> bool:
        """
        Pull/download a model

        Args:
            model_name: Name of model to pull

        Returns:
            True if successful
        """
        try:
            logger.info(f"Pulling model {model_name}... (this may take a while)")

            response = requests.post(
                f"{self.base_url}/api/pull",
                json={"name": model_name},
                stream=True,
                timeout=600
            )

            # Stream progress
            for line in response.iter_lines():
                if line:
                    status = json.loads(line)
                    if 'status' in status:
                        logger.info(f"Pull status: {status['status']}")

            logger.info(f"Model {model_name} pulled successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to pull model {model_name}: {e}")
            return False


class UniversalDataAnalyzer:
    """
    Universal data analyzer using Ollama LLM

    Can understand and categorize ANY type of data, not just predefined categories
    """

    def __init__(self, ollama_client: OllamaClient):
        """
        Initialize analyzer with Ollama client

        Args:
            ollama_client: OllamaClient instance
        """
        self.client = ollama_client
        self.analysis_cache = {}
        logger.info("Universal Data Analyzer initialized")

    def analyze_data_sample(
        self,
        sample_records: List[Dict],
        max_samples: int = 5
    ) -> Dict[str, Any]:
        """
        Analyze sample records to understand data type and domain

        Args:
            sample_records: List of sample data records
            max_samples: Maximum samples to analyze

        Returns:
            Analysis results with domain, category, entity type, etc.
        """
        if not sample_records:
            return {"error": "No sample records provided"}

        # Take subset of samples
        samples = sample_records[:max_samples]

        # Extract field names and sample values
        all_fields = set()
        field_samples = {}

        for record in samples:
            for key, value in record.items():
                all_fields.add(key)
                if key not in field_samples:
                    field_samples[key] = []
                field_samples[key].append(str(value)[:100])  # Limit value length

        # Create analysis prompt
        prompt = self._create_analysis_prompt(list(all_fields), field_samples, samples)

        system_prompt = """You are a data expert who can identify any type of data domain.
Analyze the provided data and respond ONLY with valid JSON (no markdown, no explanation).

Your response must be a JSON object with these exact keys:
{
  "domain": "the business/technical domain (e.g., ecommerce, healthcare, finance, iot, etc.)",
  "category": "more specific category within domain",
  "entity_type": "what this data represents (e.g., products, patients, transactions, sensors)",
  "confidence": 0.95,
  "field_interpretations": {
    "field_name": {"meaning": "what it represents", "data_type": "detected type"}
  },
  "suggested_source": "suggested database source name",
  "data_characteristics": ["characteristic1", "characteristic2"],
  "recommended_indexes": ["field1", "field2"],
  "retention_recommendation": {"days": 365, "reason": "why this retention period"}
}"""

        try:
            response = self.client.generate(
                prompt=prompt,
                system_prompt=system_prompt,
                temperature=0.3,  # Lower temperature for more consistent output
                max_tokens=1500
            )

            response_text = response.get('response', '').strip()

            # Try to extract JSON from response
            analysis = self._extract_json(response_text)

            if analysis:
                # Cache the analysis
                cache_key = str(sorted(all_fields))
                self.analysis_cache[cache_key] = analysis

                logger.info(
                    f"Analyzed data: domain={analysis.get('domain')}, "
                    f"entity={analysis.get('entity_type')}"
                )

                return analysis
            else:
                logger.warning("Failed to parse LLM response as JSON")
                return self._fallback_analysis(all_fields, samples)

        except Exception as e:
            logger.error(f"Data analysis failed: {e}")
            return self._fallback_analysis(all_fields, samples)

    def _create_analysis_prompt(
        self,
        fields: List[str],
        field_samples: Dict[str, List[str]],
        records: List[Dict]
    ) -> str:
        """Create detailed prompt for data analysis"""

        # Format field information
        field_info = []
        for field in fields[:15]:  # Limit to 15 fields
            samples = field_samples.get(field, [])[:3]
            field_info.append(f"  - {field}: {samples}")

        field_text = "\n".join(field_info)

        # Format sample records
        record_text = json.dumps(records[:2], indent=2)

        prompt = f"""Analyze this data and identify its domain and characteristics.

Fields found:
{field_text}

Sample records:
{record_text}

Identify:
1. What domain/industry is this data from?
2. What category within that domain?
3. What entity type does this represent?
4. What do each field likely represent?
5. What's the appropriate retention period?

Respond with JSON only."""

        return prompt

    def _extract_json(self, text: str) -> Optional[Dict]:
        """Extract JSON from LLM response"""
        try:
            # Try direct parsing
            return json.loads(text)
        except:
            pass

        # Try to find JSON in markdown code blocks
        import re
        json_pattern = r'```(?:json)?\s*(\{.*?\})\s*```'
        matches = re.findall(json_pattern, text, re.DOTALL)

        for match in matches:
            try:
                return json.loads(match)
            except:
                continue

        # Try to find JSON object
        json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
        matches = re.findall(json_pattern, text, re.DOTALL)

        for match in matches:
            try:
                return json.loads(match)
            except:
                continue

        return None

    def _fallback_analysis(
        self,
        fields: List[str],
        samples: List[Dict]
    ) -> Dict[str, Any]:
        """Fallback analysis when LLM fails"""
        return {
            "domain": "unknown",
            "category": "general_data",
            "entity_type": "data",
            "confidence": 0.5,
            "field_interpretations": {
                field: {"meaning": "unknown", "data_type": "string"}
                for field in fields[:10]
            },
            "suggested_source": "general_data",
            "data_characteristics": ["unstructured"],
            "recommended_indexes": fields[:3] if fields else [],
            "retention_recommendation": {"days": 365, "reason": "default retention"},
            "note": "LLM analysis failed, using fallback"
        }

    def detect_relationships(
        self,
        source1_fields: List[str],
        source2_fields: List[str],
        source1_name: str = "dataset1",
        source2_name: str = "dataset2"
    ) -> List[Dict[str, Any]]:
        """
        Detect potential relationships between two datasets

        Args:
            source1_fields: Fields from first dataset
            source2_fields: Fields from second dataset
            source1_name: Name of first dataset
            source2_name: Name of second dataset

        Returns:
            List of detected relationships
        """
        prompt = f"""Analyze potential relationships between these two datasets:

Dataset "{source1_name}" fields: {', '.join(source1_fields[:20])}
Dataset "{source2_name}" fields: {', '.join(source2_fields[:20])}

Identify potential foreign key relationships, common fields, or logical connections.

Respond with JSON array of relationships:
[
  {{
    "field1": "field from dataset1",
    "field2": "field from dataset2",
    "relationship_type": "foreign_key|common_attribute|derived",
    "confidence": 0.85,
    "reasoning": "why this relationship exists"
  }}
]

JSON only, no explanation."""

        try:
            response = self.client.generate(
                prompt=prompt,
                temperature=0.3,
                max_tokens=1000
            )

            result = self._extract_json(response.get('response', ''))

            if isinstance(result, list):
                return result
            elif isinstance(result, dict) and 'relationships' in result:
                return result['relationships']
            else:
                return []

        except Exception as e:
            logger.error(f"Relationship detection failed: {e}")
            return []

    def suggest_transformations(
        self,
        source_schema: Dict[str, Any],
        target_domain: str
    ) -> List[Dict[str, Any]]:
        """
        Suggest transformations based on source schema and target domain

        Args:
            source_schema: Source data schema
            target_domain: Target domain/use case

        Returns:
            List of transformation suggestions
        """
        schema_text = json.dumps(source_schema, indent=2)

        prompt = f"""Given this source schema:
{schema_text}

Suggest transformations to optimize for: {target_domain}

Respond with JSON array:
[
  {{
    "field": "field_name",
    "transformation": "description",
    "reason": "why this helps",
    "example": "before -> after"
  }}
]

JSON only."""

        try:
            response = self.client.generate(
                prompt=prompt,
                temperature=0.4,
                max_tokens=1000
            )

            result = self._extract_json(response.get('response', ''))

            if isinstance(result, list):
                return result
            else:
                return []

        except Exception as e:
            logger.error(f"Transformation suggestion failed: {e}")
            return []

    def generate_data_quality_report(
        self,
        schema: Dict[str, Any],
        sample_data: List[Dict],
        stats: Dict[str, Any]
    ) -> str:
        """
        Generate natural language data quality report

        Args:
            schema: Data schema
            sample_data: Sample records
            stats: Statistics about the data

        Returns:
            Natural language report
        """
        prompt = f"""Generate a data quality report for this dataset:

Schema: {json.dumps(schema, indent=2)[:500]}
Statistics: {json.dumps(stats, indent=2)}
Sample: {json.dumps(sample_data[:2], indent=2)}

Provide:
1. Overall data quality assessment
2. Potential issues or anomalies
3. Recommendations for improvement
4. Data completeness analysis

Keep it concise (3-4 paragraphs)."""

        try:
            response = self.client.generate(
                prompt=prompt,
                temperature=0.5,
                max_tokens=800
            )

            return response.get('response', 'Failed to generate report')

        except Exception as e:
            logger.error(f"Quality report generation failed: {e}")
            return f"Error generating report: {e}"


# Global instance (lazy-loaded)
_ollama_client = None
_universal_analyzer = None


def get_ollama_client(model: str = "llama2") -> OllamaClient:
    """Get or create Ollama client instance"""
    global _ollama_client
    if _ollama_client is None:
        _ollama_client = OllamaClient(model=model)
    return _ollama_client


def get_universal_analyzer() -> UniversalDataAnalyzer:
    """Get or create Universal Data Analyzer instance"""
    global _universal_analyzer
    if _universal_analyzer is None:
        client = get_ollama_client()
        _universal_analyzer = UniversalDataAnalyzer(client)
    return _universal_analyzer
