# nl_query_interface.py - Natural Language to Query Conversion

"""
Natural Language Query Interface using Ollama

Converts natural language questions to database queries:
- "Show me all products under $50"
- "Find employees in the engineering department"
- "What are the top 5 highest revenue items?"

Supports:
- MongoDB queries
- SQL queries (for future SQL support)
- Aggregation pipelines
"""

import logging
import json
import re
from typing import Dict, List, Any, Optional
from ollama_integration import get_ollama_client

logger = logging.getLogger(__name__)


class NaturalLanguageQueryEngine:
    """Convert natural language to database queries"""

    def __init__(self):
        """Initialize NL query engine"""
        self.client = get_ollama_client()
        self.query_cache = {}
        logger.info("Natural Language Query Engine initialized")

    def nl_to_mongodb_query(
        self,
        natural_query: str,
        schema: Dict[str, Any],
        collection_name: str
    ) -> Dict[str, Any]:
        """
        Convert natural language to MongoDB query

        Args:
            natural_query: Natural language question
            schema: Data schema
            collection_name: Collection name

        Returns:
            MongoDB query dict
        """
        # Check cache
        cache_key = f"{natural_query}_{collection_name}"
        if cache_key in self.query_cache:
            logger.info(f"Using cached query for: {natural_query}")
            return self.query_cache[cache_key]

        # Format schema for prompt
        schema_fields = {
            k: v.get('type', 'unknown')
            for k, v in schema.items()
            if not k.startswith('_')
        }

        prompt = f"""Convert this natural language query to MongoDB query.

Collection: {collection_name}
Schema: {json.dumps(schema_fields, indent=2)}

Natural language query: "{natural_query}"

Generate a MongoDB query that answers this question.

Respond with JSON only (no explanation):
{{
  "query": {{}},
  "sort": {{}},
  "limit": 0,
  "explanation": "brief explanation"
}}

Examples:
- "products under $50" → {{"query": {{"price": {{"$lt": 50}}}}, "limit": 0}}
- "top 5 by price" → {{"query": {{}}, "sort": {{"price": -1}}, "limit": 5}}
- "items containing laptop" → {{"query": {{"name": {{"$regex": "laptop", "$options": "i"}}}}, "limit": 0}}

JSON only:"""

        try:
            response = self.client.generate(
                prompt=prompt,
                temperature=0.2,  # Low temperature for consistent queries
                max_tokens=500
            )

            result_text = response.get('response', '')

            # Extract JSON
            query_result = self._extract_json(result_text)

            if query_result:
                # Cache the query
                self.query_cache[cache_key] = query_result
                logger.info(f"Generated query for '{natural_query}': {query_result.get('query')}")
                return query_result
            else:
                logger.warning(f"Failed to parse query from LLM response")
                return self._fallback_query(natural_query, schema_fields)

        except Exception as e:
            logger.error(f"NL to MongoDB query conversion failed: {e}")
            return self._fallback_query(natural_query, schema_fields)

    def nl_to_sql_query(
        self,
        natural_query: str,
        schema: Dict[str, Any],
        table_name: str
    ) -> str:
        """
        Convert natural language to SQL query

        Args:
            natural_query: Natural language question
            schema: Data schema
            table_name: Table name

        Returns:
            SQL query string
        """
        schema_fields = {
            k: v.get('type', 'unknown')
            for k, v in schema.items()
            if not k.startswith('_')
        }

        prompt = f"""Convert this natural language query to SQL.

Table: {table_name}
Schema: {json.dumps(schema_fields, indent=2)}

Natural language query: "{natural_query}"

Generate a valid SQL SELECT query.

Respond with just the SQL query, no explanation."""

        try:
            response = self.client.generate(
                prompt=prompt,
                temperature=0.2,
                max_tokens=300
            )

            sql_query = response.get('response', '').strip()

            # Extract SQL if wrapped in code blocks
            sql_pattern = r'```sql\s*(.*?)\s*```'
            matches = re.findall(sql_pattern, sql_query, re.DOTALL | re.IGNORECASE)

            if matches:
                sql_query = matches[0].strip()

            # Remove any remaining markdown
            sql_query = sql_query.replace('```', '').strip()

            logger.info(f"Generated SQL: {sql_query}")
            return sql_query

        except Exception as e:
            logger.error(f"NL to SQL conversion failed: {e}")
            return f"SELECT * FROM {table_name} LIMIT 10"

    def explain_query_results(
        self,
        natural_query: str,
        results: List[Dict],
        limit: int = 3
    ) -> str:
        """
        Explain query results in natural language

        Args:
            natural_query: Original natural language question
            results: Query results
            limit: Number of results to show in explanation

        Returns:
            Natural language explanation
        """
        if not results:
            return f"No results found for: {natural_query}"

        result_sample = results[:limit]

        prompt = f"""Explain these query results in natural language.

Original question: "{natural_query}"

Results found: {len(results)} records

Sample results:
{json.dumps(result_sample, indent=2)}

Provide a brief, conversational summary (2-3 sentences) of what was found."""

        try:
            response = self.client.generate(
                prompt=prompt,
                temperature=0.5,
                max_tokens=200
            )

            explanation = response.get('response', '').strip()
            return explanation

        except Exception as e:
            logger.error(f"Result explanation failed: {e}")
            return f"Found {len(results)} results for: {natural_query}"

    def suggest_related_queries(
        self,
        query: str,
        schema: Dict[str, Any]
    ) -> List[str]:
        """
        Suggest related queries based on user's question

        Args:
            query: User's natural language query
            schema: Data schema

        Returns:
            List of suggested follow-up queries
        """
        schema_fields = list(schema.keys())[:20]

        prompt = f"""Based on this query: "{query}"

Available fields: {', '.join(schema_fields)}

Suggest 5 related follow-up questions the user might want to ask.

Respond with JSON array:
["question1", "question2", "question3", "question4", "question5"]

JSON only."""

        try:
            response = self.client.generate(
                prompt=prompt,
                temperature=0.6,
                max_tokens=300
            )

            result = self._extract_json(response.get('response', ''))

            if isinstance(result, list):
                return result[:5]
            else:
                return []

        except Exception as e:
            logger.error(f"Query suggestion failed: {e}")
            return []

    def _extract_json(self, text: str) -> Optional[Any]:
        """Extract JSON from LLM response"""
        try:
            return json.loads(text)
        except:
            pass

        # Try markdown code blocks
        json_pattern = r'```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```'
        matches = re.findall(json_pattern, text, re.DOTALL)

        for match in matches:
            try:
                return json.loads(match)
            except:
                continue

        # Try finding JSON directly
        patterns = [
            r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}',
            r'\[.*?\]'
        ]

        for pattern in patterns:
            matches = re.findall(pattern, text, re.DOTALL)
            for match in matches:
                try:
                    return json.loads(match)
                except:
                    continue

        return None

    def _fallback_query(
        self,
        natural_query: str,
        schema_fields: Dict[str, str]
    ) -> Dict[str, Any]:
        """Generate fallback query when LLM fails"""
        # Simple keyword-based fallback
        query_lower = natural_query.lower()

        # Extract number if present
        import re
        numbers = re.findall(r'\d+', query_lower)

        # Look for comparison keywords
        if 'under' in query_lower or 'less than' in query_lower or '<' in query_lower:
            if numbers and 'price' in schema_fields:
                return {
                    "query": {"price": {"$lt": int(numbers[0])}},
                    "limit": 0,
                    "explanation": "Fallback: searching for price less than value"
                }

        if 'over' in query_lower or 'more than' in query_lower or '>' in query_lower:
            if numbers and 'price' in schema_fields:
                return {
                    "query": {"price": {"$gt": int(numbers[0])}},
                    "limit": 0,
                    "explanation": "Fallback: searching for price greater than value"
                }

        # Top N queries
        if 'top' in query_lower and numbers:
            limit = int(numbers[0])
            # Try to find sortable field
            sort_field = None
            for field in schema_fields:
                if field in ['price', 'amount', 'revenue', 'score', 'rating']:
                    sort_field = field
                    break

            if sort_field:
                return {
                    "query": {},
                    "sort": {sort_field: -1},
                    "limit": limit,
                    "explanation": f"Fallback: top {limit} by {sort_field}"
                }

        # Default: return all
        return {
            "query": {},
            "limit": 100,
            "explanation": "Fallback: returning all records (limited to 100)"
        }


# Global instance
_nl_query_engine = None


def get_nl_query_engine() -> NaturalLanguageQueryEngine:
    """Get or create NL query engine instance"""
    global _nl_query_engine
    if _nl_query_engine is None:
        _nl_query_engine = NaturalLanguageQueryEngine()
    return _nl_query_engine
