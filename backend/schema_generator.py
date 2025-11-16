"""
Multi-Database Schema Generator
Generates schemas for PostgreSQL, MongoDB, Neo4j, and JSON Schema
Handles type conflicts, union types, and schema evolution
"""

import logging
from collections import defaultdict, Counter
from datetime import datetime
import hashlib
import json

try:
    import pandas as pd
    SCHEMA_AVAILABLE = True
except ImportError:
    SCHEMA_AVAILABLE = False


class TypeInference:
    """Infer and handle types including conflicts and unions"""

    @staticmethod
    def infer_type(value):
        """Infer type from value"""
        if value is None or value == '' or (isinstance(value, str) and value.strip().lower() in ['null', 'n/a', 'none']):
            return 'null'

        str_val = str(value).strip()

        # Boolean
        if str_val.lower() in ['true', 'false', 'yes', 'no', '1', '0']:
            return 'boolean'

        # Integer
        try:
            int(str_val)
            if '.' not in str_val:
                return 'integer'
        except (ValueError, TypeError):
            pass

        # Float
        try:
            float(str_val)
            return 'float'
        except (ValueError, TypeError):
            pass

        # Date patterns
        import re
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'^\d{2}/\d{2}/\d{4}',  # DD/MM/YYYY or MM/DD/YYYY
            r'^\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
            r'^\w{3}\s+\d{1,2},\s+\d{4}',  # Mon DD, YYYY
        ]
        for pattern in date_patterns:
            if re.match(pattern, str_val):
                return 'date'

        # Email
        if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str_val):
            return 'email'

        # URL
        if re.match(r'^https?://', str_val):
            return 'url'

        # Default to string
        return 'string'

    @staticmethod
    def resolve_type_conflict(types_list):
        """
        Resolve type conflicts when field has multiple types
        Returns union type or most general type with normalization strategy
        """
        type_counts = Counter(types_list)

        # Remove nulls for analysis
        non_null_types = [t for t in types_list if t != 'null']
        has_null = 'null' in type_counts

        if not non_null_types:
            return {
                'type': 'null',
                'nullable': True,
                'union_types': [],
                'normalization': None
            }

        unique_types = set(non_null_types)

        # Single type (ignoring nulls)
        if len(unique_types) == 1:
            return {
                'type': list(unique_types)[0],
                'nullable': has_null,
                'union_types': [],
                'normalization': None
            }

        # Type hierarchy: string > float > integer
        if unique_types <= {'integer', 'float'}:
            return {
                'type': 'float',
                'nullable': has_null,
                'union_types': list(unique_types),
                'normalization': 'cast_to_float'
            }

        if unique_types <= {'integer', 'float', 'string'}:
            return {
                'type': 'string',
                'nullable': has_null,
                'union_types': list(unique_types),
                'normalization': 'cast_to_string'
            }

        # Multiple incompatible types - create union
        return {
            'type': 'union',
            'nullable': has_null,
            'union_types': list(unique_types),
            'normalization': 'variant_column_or_jsonb'
        }


class MultiDBSchemaGenerator:
    """Generate schemas for multiple database types"""

    def __init__(self):
        self.type_mapper = TypeInference()

    def generate_schema(self, records, source_id):
        """
        Generate unified schema from records
        Returns schema with metadata for multiple DBs
        """
        if not records:
            return None

        # Analyze fields across all records
        field_analysis = self._analyze_fields(records)

        # Generate schema metadata
        schema_id = self._generate_schema_id(field_analysis)

        schema = {
            'schema_id': schema_id,
            'source_id': source_id,
            'generated_at': datetime.now().isoformat(),
            'compatible_dbs': ['postgresql', 'mongodb', 'neo4j', 'json_schema'],
            'fields': [],
            'primary_key_candidates': [],
            'indexes_suggested': [],
            'migration_notes': [],
            'data_quality': {}
        }

        # Generate field definitions
        for field_name, field_info in field_analysis.items():
            field_def = self._generate_field_definition(field_name, field_info)
            schema['fields'].append(field_def)

        # Identify primary key candidates
        schema['primary_key_candidates'] = self._identify_primary_keys(field_analysis)

        # Suggest indexes
        schema['indexes_suggested'] = self._suggest_indexes(field_analysis)

        # Data quality metrics
        schema['data_quality'] = self._calculate_quality_metrics(field_analysis, len(records))

        return schema

    def _analyze_fields(self, records):
        """Analyze all fields across records"""
        field_stats = defaultdict(lambda: {
            'values': [],
            'types': [],
            'null_count': 0,
            'occurrences': 0,
            'examples': [],
            'paths': set()
        })

        for record in records:
            for key, value in record.items():
                # Skip metadata fields
                if key.startswith('_'):
                    continue

                field_stats[key]['values'].append(value)
                field_stats[key]['types'].append(self.type_mapper.infer_type(value))
                field_stats[key]['occurrences'] += 1

                if value is None or value == '' or str(value).strip().lower() in ['null', 'n/a']:
                    field_stats[key]['null_count'] += 1

                if len(field_stats[key]['examples']) < 3:
                    field_stats[key]['examples'].append(value)

                # Track path if nested
                if '.' in key or '[' in key:
                    field_stats[key]['paths'].add(key)

        return dict(field_stats)

    def _generate_field_definition(self, field_name, field_info):
        """Generate field definition with type resolution"""
        types_list = field_info['types']
        type_resolution = self.type_mapper.resolve_type_conflict(types_list)

        total_values = len(field_info['values'])
        null_count = field_info['null_count']
        completeness = (total_values - null_count) / total_values if total_values > 0 else 0

        field_def = {
            'name': field_name,
            'path': f"$.{field_name}",
            'type': type_resolution['type'],
            'nullable': type_resolution['nullable'] or (null_count > 0),
            'null_percentage': (null_count / total_values * 100) if total_values > 0 else 0,
            'completeness': completeness,
            'example_values': field_info['examples'][:3],
            'occurrences': field_info['occurrences'],
            'confidence': self._calculate_confidence(field_info, type_resolution),
            'source_offsets': list(field_info.get('paths', set()))
        }

        # Add union type info if applicable
        if type_resolution['union_types']:
            field_def['union_types'] = type_resolution['union_types']
            field_def['normalization_strategy'] = type_resolution['normalization']
            field_def['data_quality_note'] = 'Mixed types detected - normalization recommended'

        # Add suggested index based on heuristics
        if self._should_index(field_name, field_info):
            field_def['suggested_index'] = True

        return field_def

    def _calculate_confidence(self, field_info, type_resolution):
        """Calculate confidence score for field type"""
        if not field_info['types']:
            return 0.0

        # If all types agree, high confidence
        type_counts = Counter(field_info['types'])
        non_null_types = [t for t in field_info['types'] if t != 'null']

        if not non_null_types:
            return 0.5

        dominant_type = max(Counter(non_null_types).items(), key=lambda x: x[1])
        type_consistency = dominant_type[1] / len(non_null_types)

        # Factor in completeness
        completeness = 1 - (field_info['null_count'] / len(field_info['values']))

        # Combined confidence
        confidence = (type_consistency * 0.7) + (completeness * 0.3)

        return round(confidence, 2)

    def _should_index(self, field_name, field_info):
        """Determine if field should be indexed"""
        # Index candidates: id fields, high cardinality, frequently queried
        index_patterns = ['id', 'key', 'code', 'slug', 'email', 'username']

        if any(pattern in field_name.lower() for pattern in index_patterns):
            return True

        # High cardinality (unique values)
        unique_ratio = len(set(field_info['values'])) / len(field_info['values'])
        if unique_ratio > 0.9:
            return True

        return False

    def _identify_primary_keys(self, field_analysis):
        """Identify primary key candidates"""
        candidates = []

        for field_name, field_info in field_analysis.items():
            # Check for id-like names
            if 'id' in field_name.lower() or 'key' in field_name.lower():
                unique_ratio = len(set(field_info['values'])) / len(field_info['values'])
                if unique_ratio > 0.95:  # Highly unique
                    candidates.append({
                        'field': field_name,
                        'uniqueness': unique_ratio,
                        'reason': 'high_uniqueness_and_naming'
                    })

        return candidates

    def _suggest_indexes(self, field_analysis):
        """Suggest indexes for query optimization"""
        indexes = []

        for field_name, field_info in field_analysis.items():
            if self._should_index(field_name, field_info):
                indexes.append({
                    'field': field_name,
                    'type': 'btree',  # Default
                    'reason': 'Frequently queried or high cardinality'
                })

        return indexes

    def _calculate_quality_metrics(self, field_analysis, total_records):
        """Calculate overall data quality metrics"""
        total_fields = len(field_analysis)
        fields_with_nulls = sum(1 for f in field_analysis.values() if f['null_count'] > 0)
        fields_with_mixed_types = sum(1 for f in field_analysis.values()
                                      if len(set(t for t in f['types'] if t != 'null')) > 1)

        avg_completeness = sum(
            (len(f['values']) - f['null_count']) / len(f['values'])
            for f in field_analysis.values()
        ) / total_fields if total_fields > 0 else 0

        return {
            'total_fields': total_fields,
            'total_records': total_records,
            'fields_with_nulls': fields_with_nulls,
            'fields_with_mixed_types': fields_with_mixed_types,
            'average_completeness': round(avg_completeness, 2),
            'quality_score': round(avg_completeness * 100, 2)
        }

    def _generate_schema_id(self, field_analysis):
        """Generate deterministic schema ID"""
        # Create hash from field names and types
        field_signature = sorted([
            f"{name}:{sorted(set(info['types']))}"
            for name, info in field_analysis.items()
        ])

        signature_str = '|'.join(field_signature)
        schema_hash = hashlib.md5(signature_str.encode()).hexdigest()[:12]

        return f"schema_{schema_hash}"

    def generate_postgresql_ddl(self, schema):
        """Generate PostgreSQL DDL from schema"""
        table_name = schema['source_id'].replace('-', '_').replace('.', '_')

        ddl_lines = [f"CREATE TABLE IF NOT EXISTS {table_name} ("]

        # Add fields
        for field in schema['fields']:
            field_name = field['name'].replace('.', '_').replace('[', '_').replace(']', '')
            pg_type = self._map_to_postgresql_type(field)

            nullable = "" if field['nullable'] else " NOT NULL"
            ddl_lines.append(f"    {field_name} {pg_type}{nullable},")

        # Add primary key if identified
        if schema['primary_key_candidates']:
            pk_field = schema['primary_key_candidates'][0]['field'].replace('.', '_')
            ddl_lines.append(f"    PRIMARY KEY ({pk_field}),")

        # Remove trailing comma
        ddl_lines[-1] = ddl_lines[-1].rstrip(',')

        ddl_lines.append(");")

        # Add indexes
        ddl = '\n'.join(ddl_lines)

        for idx in schema['indexes_suggested']:
            idx_name = f"idx_{table_name}_{idx['field'].replace('.', '_')}"
            idx_field = idx['field'].replace('.', '_')
            ddl += f"\n\nCREATE INDEX IF NOT EXISTS {idx_name} ON {table_name}({idx_field});"

        return ddl

    def _map_to_postgresql_type(self, field):
        """Map field type to PostgreSQL type"""
        field_type = field['type']

        type_map = {
            'integer': 'INTEGER',
            'float': 'DOUBLE PRECISION',
            'boolean': 'BOOLEAN',
            'date': 'TIMESTAMP',
            'email': 'VARCHAR(255)',
            'url': 'TEXT',
            'string': 'TEXT',
            'null': 'TEXT',  # Default for null
            'union': 'JSONB'  # Store union types as JSONB
        }

        return type_map.get(field_type, 'TEXT')

    def generate_mongodb_schema(self, schema):
        """Generate MongoDB JSON Schema"""
        properties = {}
        required = []

        for field in schema['fields']:
            field_name = field['name']

            mongo_field = {
                'bsonType': self._map_to_mongodb_type(field),
                'description': f"Type: {field['type']}, Confidence: {field['confidence']}"
            }

            if field.get('union_types'):
                mongo_field['bsonType'] = ['string', 'int', 'double', 'null']  # Union
                mongo_field['note'] = f"Mixed types: {field['union_types']}"

            properties[field_name] = mongo_field

            if not field['nullable'] and field['completeness'] > 0.9:
                required.append(field_name)

        mongo_schema = {
            '$jsonSchema': {
                'bsonType': 'object',
                'required': required,
                'properties': properties
            }
        }

        return mongo_schema

    def _map_to_mongodb_type(self, field):
        """Map field type to MongoDB BSON type"""
        type_map = {
            'integer': 'int',
            'float': 'double',
            'boolean': 'bool',
            'date': 'date',
            'string': 'string',
            'email': 'string',
            'url': 'string',
            'null': 'null'
        }

        return type_map.get(field['type'], 'string')

    def generate_neo4j_schema(self, schema):
        """Generate Neo4j Cypher schema/constraints"""
        label = schema['source_id'].replace('-', '_').title()

        cypher_statements = [
            f"// Neo4j Schema for {label}",
            f"// Node label: {label}\n"
        ]

        # Create uniqueness constraints for PK candidates
        for pk in schema['primary_key_candidates']:
            pk_field = pk['field']
            cypher_statements.append(
                f"CREATE CONSTRAINT {label}_{pk_field}_unique IF NOT EXISTS\n"
                f"FOR (n:{label}) REQUIRE n.{pk_field} IS UNIQUE;"
            )

        # Create property existence constraints for required fields
        for field in schema['fields']:
            if not field['nullable'] and field['completeness'] > 0.9:
                field_name = field['name']
                cypher_statements.append(
                    f"CREATE CONSTRAINT {label}_{field_name}_exists IF NOT EXISTS\n"
                    f"FOR (n:{label}) REQUIRE n.{field_name} IS NOT NULL;"
                )

        # Create indexes
        for idx in schema['indexes_suggested']:
            idx_field = idx['field']
            cypher_statements.append(
                f"CREATE INDEX {label}_{idx_field}_idx IF NOT EXISTS\n"
                f"FOR (n:{label}) ON (n.{idx_field});"
            )

        return '\n\n'.join(cypher_statements)

    def generate_json_schema(self, schema):
        """Generate JSON Schema (draft 7)"""
        properties = {}
        required = []

        for field in schema['fields']:
            field_name = field['name']

            json_field = {
                'type': self._map_to_json_schema_type(field),
                'description': f"Confidence: {field['confidence']}"
            }

            if field.get('example_values'):
                json_field['examples'] = field['example_values']

            if field.get('union_types'):
                json_field['type'] = field['union_types']

            properties[field_name] = json_field

            if not field['nullable']:
                required.append(field_name)

        json_schema = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': properties,
            'required': required
        }

        return json_schema

    def _map_to_json_schema_type(self, field):
        """Map to JSON Schema type"""
        type_map = {
            'integer': 'integer',
            'float': 'number',
            'boolean': 'boolean',
            'string': 'string',
            'date': 'string',  # with format: date-time
            'email': 'string',  # with format: email
            'url': 'string',  # with format: uri
            'null': 'null'
        }

        return type_map.get(field['type'], 'string')


# Global instance
multi_db_schema_generator = MultiDBSchemaGenerator()
