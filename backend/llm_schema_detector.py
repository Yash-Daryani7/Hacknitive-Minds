# llm_schema_detector.py - LLM-Powered Universal Schema Detection

"""
Universal Schema Detection using Ollama LLM

Can detect and understand ANY type of data structure from ANY domain:
- E-commerce, Healthcare, Finance, IoT, Social Media, etc.
- Scientific data, Government data, Gaming data, etc.
- ANY custom domain not predefined

Features:
1. Dynamic field type detection (not limited to predefined types)
2. Semantic meaning extraction
3. Domain-specific understanding
4. Relationship inference
5. Data quality assessment
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
import json
from datetime import datetime
from ollama_integration import get_ollama_client, get_universal_analyzer

logger = logging.getLogger(__name__)


class LLMSchemaDetector:
    """
    Advanced schema detector using LLM for universal data understanding
    """

    def __init__(self, use_llm: bool = True, fallback: bool = True):
        """
        Initialize LLM Schema Detector

        Args:
            use_llm: Whether to use LLM (if False, uses traditional methods)
            fallback: Whether to fallback to traditional methods if LLM fails
        """
        self.use_llm = use_llm
        self.fallback = fallback

        if use_llm:
            try:
                self.analyzer = get_universal_analyzer()
                self.client = get_ollama_client()
                logger.info("LLM Schema Detector initialized with Ollama")
            except Exception as e:
                logger.warning(f"Could not initialize LLM: {e}")
                if not fallback:
                    raise
                self.use_llm = False
                logger.info("Falling back to traditional schema detection")

    def detect_comprehensive_schema(
        self,
        data_batch: List[Dict],
        existing_schema: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Detect comprehensive schema with LLM understanding

        Args:
            data_batch: Batch of data records
            existing_schema: Existing schema to evolve

        Returns:
            Comprehensive schema with semantic information
        """
        if not data_batch:
            return existing_schema or {}

        # Analyze data using LLM
        if self.use_llm:
            try:
                analysis = self.analyzer.analyze_data_sample(data_batch)
                schema = self._build_schema_from_analysis(data_batch, analysis)
                return schema
            except Exception as e:
                logger.error(f"LLM schema detection failed: {e}")
                if not self.fallback:
                    raise

        # Fallback to traditional detection
        return self._traditional_schema_detection(data_batch, existing_schema)

    def _build_schema_from_analysis(
        self,
        data_batch: List[Dict],
        analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Build comprehensive schema from LLM analysis

        Args:
            data_batch: Data records
            analysis: LLM analysis results

        Returns:
            Rich schema with semantic information
        """
        schema = {}

        # Extract all fields
        all_fields = set()
        field_values = {}

        for record in data_batch:
            for key, value in record.items():
                all_fields.add(key)
                if key not in field_values:
                    field_values[key] = []
                field_values[key].append(value)

        # Get field interpretations from LLM
        field_interpretations = analysis.get('field_interpretations', {})

        # Build schema for each field
        for field in all_fields:
            values = field_values[field]

            # Get LLM interpretation
            interpretation = field_interpretations.get(field, {})

            # Detect data type
            data_type = self._detect_field_type(values)

            # Build field schema
            schema[field] = {
                'type': interpretation.get('data_type', data_type),
                'semantic_meaning': interpretation.get('meaning', 'unknown'),
                'sample_values': [str(v)[:100] for v in values[:3] if v is not None],
                'null_count': sum(1 for v in values if v is None or v == ''),
                'total_count': len(values),
                'completeness': 1 - (sum(1 for v in values if v is None or v == '') / len(values)),
                'unique_count': len(set(str(v) for v in values if v is not None)),
                'llm_detected': True,
            }

        # Add metadata from analysis
        schema['_metadata'] = {
            'domain': analysis.get('domain', 'unknown'),
            'category': analysis.get('category', 'general'),
            'entity_type': analysis.get('entity_type', 'data'),
            'confidence': analysis.get('confidence', 0.5),
            'suggested_source': analysis.get('suggested_source', 'general_data'),
            'data_characteristics': analysis.get('data_characteristics', []),
            'recommended_indexes': analysis.get('recommended_indexes', []),
            'retention_recommendation': analysis.get('retention_recommendation', {}),
            'detected_at': datetime.now().isoformat(),
        }

        logger.info(
            f"LLM detected schema: domain={schema['_metadata']['domain']}, "
            f"entity={schema['_metadata']['entity_type']}, "
            f"fields={len(schema)-1}"
        )

        return schema

    def _detect_field_type(self, values: List[Any]) -> str:
        """Detect field type from values"""
        import re

        non_null_values = [v for v in values if v is not None and v != '']

        if not non_null_values:
            return 'null'

        # Sample a few values
        sample = non_null_values[:10]

        # Type detection logic
        int_count = 0
        float_count = 0
        bool_count = 0
        date_count = 0
        email_count = 0
        url_count = 0

        for val in sample:
            str_val = str(val).strip()

            # Boolean
            if str_val.lower() in ['true', 'false', 'yes', 'no', '1', '0']:
                bool_count += 1

            # Email
            if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str_val):
                email_count += 1

            # URL
            if re.match(r'^https?://[^\s]+$', str_val):
                url_count += 1

            # Integer
            try:
                int(str_val)
                if '.' not in str_val:
                    int_count += 1
            except:
                pass

            # Float
            try:
                float(str_val)
                float_count += 1
            except:
                pass

            # Date
            if re.match(r'^\d{4}-\d{2}-\d{2}', str_val) or \
               re.match(r'^\d{2}/\d{2}/\d{4}', str_val):
                date_count += 1

        # Determine type based on counts
        total = len(sample)

        if email_count / total > 0.7:
            return 'email'
        if url_count / total > 0.7:
            return 'url'
        if bool_count / total > 0.7:
            return 'boolean'
        if date_count / total > 0.7:
            return 'date'
        if int_count / total > 0.7:
            return 'integer'
        if float_count / total > 0.7:
            return 'float'

        return 'string'

    def _traditional_schema_detection(
        self,
        data_batch: List[Dict],
        existing_schema: Optional[Dict]
    ) -> Dict[str, Any]:
        """Traditional schema detection (fallback)"""
        from transform import infer_schema
        return infer_schema(data_batch, existing_schema or {})

    def detect_entity_relationships(
        self,
        schema1: Dict[str, Any],
        schema2: Dict[str, Any],
        name1: str = "dataset1",
        name2: str = "dataset2"
    ) -> List[Dict[str, Any]]:
        """
        Detect relationships between two schemas using LLM

        Args:
            schema1: First schema
            schema2: Second schema
            name1: Name of first dataset
            name2: Name of second dataset

        Returns:
            List of detected relationships
        """
        if not self.use_llm:
            return []

        try:
            fields1 = [k for k in schema1.keys() if not k.startswith('_')]
            fields2 = [k for k in schema2.keys() if not k.startswith('_')]

            relationships = self.analyzer.detect_relationships(
                fields1, fields2, name1, name2
            )

            return relationships

        except Exception as e:
            logger.error(f"Relationship detection failed: {e}")
            return []

    def suggest_schema_improvements(
        self,
        schema: Dict[str, Any],
        data_samples: List[Dict]
    ) -> List[Dict[str, str]]:
        """
        Suggest improvements to schema based on LLM analysis

        Args:
            schema: Current schema
            data_samples: Sample data

        Returns:
            List of improvement suggestions
        """
        if not self.use_llm:
            return []

        try:
            prompt = f"""Analyze this data schema and suggest improvements:

Schema: {json.dumps({k: v for k, v in schema.items() if not k.startswith('_')}, indent=2)[:1000]}
Sample data: {json.dumps(data_samples[:2], indent=2)}

Suggest improvements for:
1. Better field names
2. Missing validations
3. Normalization opportunities
4. Index recommendations
5. Data quality issues

Respond with JSON array:
[
  {{
    "type": "naming|validation|normalization|index|quality",
    "field": "field_name",
    "suggestion": "what to do",
    "reason": "why this helps",
    "priority": "high|medium|low"
  }}
]

JSON only."""

            response = self.client.generate(
                prompt=prompt,
                temperature=0.3,
                max_tokens=1000
            )

            result_text = response.get('response', '')

            # Extract JSON
            import re
            json_pattern = r'\[.*\]'
            matches = re.findall(json_pattern, result_text, re.DOTALL)

            for match in matches:
                try:
                    suggestions = json.loads(match)
                    if isinstance(suggestions, list):
                        return suggestions
                except:
                    continue

            return []

        except Exception as e:
            logger.error(f"Schema improvement suggestion failed: {e}")
            return []


class DynamicCategoryLearner:
    """
    Learn new data categories dynamically (not hardcoded)

    Can discover new domains like:
    - Space/Astronomy data
    - Agriculture data
    - Entertainment/Streaming data
    - Logistics/Supply chain data
    - ANY custom domain
    """

    def __init__(self):
        """Initialize dynamic category learner"""
        self.client = get_ollama_client()
        self.analyzer = get_universal_analyzer()
        self.learned_categories = {}
        logger.info("Dynamic Category Learner initialized")

    def discover_category(
        self,
        data_samples: List[Dict],
        hint: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Discover data category dynamically

        Args:
            data_samples: Sample data records
            hint: Optional hint about the domain

        Returns:
            Discovered category information
        """
        # Get comprehensive analysis
        analysis = self.analyzer.analyze_data_sample(data_samples)

        # Extract category info
        category_info = {
            'domain': analysis.get('domain', 'unknown'),
            'category': analysis.get('category', 'general'),
            'entity_type': analysis.get('entity_type', 'data'),
            'database_name': f"{analysis.get('domain', 'unknown').lower().replace(' ', '_')}_db",
            'confidence': analysis.get('confidence', 0.0),
            'characteristics': analysis.get('data_characteristics', []),
            'retention_days': analysis.get('retention_recommendation', {}).get('days', 365),
            'discovered_at': datetime.now().isoformat(),
        }

        # Cache the learned category
        category_key = category_info['domain']
        self.learned_categories[category_key] = category_info

        logger.info(f"Discovered new category: {category_info['domain']} ({category_info['category']})")

        return category_info

    def get_category_rules(self, domain: str) -> Dict[str, Any]:
        """
        Get auto-categorization rules for a learned domain

        Args:
            domain: Domain name

        Returns:
            Categorization rules
        """
        if domain in self.learned_categories:
            return self.learned_categories[domain]

        # Generate rules using LLM
        try:
            prompt = f"""For the domain "{domain}", provide categorization rules.

What field names/keywords would indicate data belongs to this domain?

Respond with JSON:
{{
  "domain": "{domain}",
  "keywords": ["keyword1", "keyword2", ...],
  "typical_fields": ["field1", "field2", ...],
  "retention_days": 365,
  "characteristics": ["char1", "char2"]
}}

JSON only."""

            response = self.client.generate(
                prompt=prompt,
                temperature=0.3,
                max_tokens=500
            )

            # Extract JSON
            import re
            result_text = response.get('response', '')
            json_pattern = r'\{.*\}'
            matches = re.findall(json_pattern, result_text, re.DOTALL)

            for match in matches:
                try:
                    rules = json.loads(match)
                    self.learned_categories[domain] = rules
                    return rules
                except:
                    continue

        except Exception as e:
            logger.error(f"Failed to generate category rules: {e}")

        return {}

    def export_learned_categories(self, filepath: str):
        """Export learned categories to file"""
        try:
            with open(filepath, 'w') as f:
                json.dump(self.learned_categories, f, indent=2)
            logger.info(f"Exported {len(self.learned_categories)} categories to {filepath}")
        except Exception as e:
            logger.error(f"Failed to export categories: {e}")

    def import_learned_categories(self, filepath: str):
        """Import previously learned categories"""
        try:
            with open(filepath, 'r') as f:
                self.learned_categories = json.load(f)
            logger.info(f"Imported {len(self.learned_categories)} categories from {filepath}")
        except Exception as e:
            logger.error(f"Failed to import categories: {e}")


# Global instances
_schema_detector = None
_category_learner = None


def get_llm_schema_detector(use_llm: bool = True) -> LLMSchemaDetector:
    """Get or create LLM schema detector instance"""
    global _schema_detector
    if _schema_detector is None:
        _schema_detector = LLMSchemaDetector(use_llm=use_llm)
    return _schema_detector


def get_category_learner() -> DynamicCategoryLearner:
    """Get or create category learner instance"""
    global _category_learner
    if _category_learner is None:
        _category_learner = DynamicCategoryLearner()
    return _category_learner
