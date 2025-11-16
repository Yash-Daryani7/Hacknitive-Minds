import logging
import re
from datetime import datetime
from collections import defaultdict

def detect_type(value):
    """Detect the data type of a value intelligently."""
    if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
        return 'null'

    # Convert to string for pattern matching
    str_value = str(value).strip()

    # Boolean detection
    if str_value.lower() in ['true', 'false', 'yes', 'no', '1', '0']:
        return 'boolean'

    # Email detection
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if re.match(email_pattern, str_value):
        return 'email'

    # Integer detection
    try:
        int_val = int(str_value)
        # Check if it's not a float disguised as int
        if '.' not in str_value:
            return 'integer'
    except (ValueError, TypeError):
        pass

    # Float detection
    try:
        float(str_value)
        return 'float'
    except (ValueError, TypeError):
        pass

    # Date detection (multiple formats)
    date_patterns = [
        r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
        r'^\d{2}/\d{2}/\d{4}$',  # DD/MM/YYYY
        r'^\d{2}-\d{2}-\d{4}$',  # DD-MM-YYYY
    ]
    for pattern in date_patterns:
        if re.match(pattern, str_value):
            return 'date'

    # URL detection
    url_pattern = r'^https?://[^\s]+$'
    if re.match(url_pattern, str_value):
        return 'url'

    # Default to string
    return 'string'

def infer_field_type(values):
    """Infer the most common type from a list of values."""
    type_counts = defaultdict(int)

    for value in values:
        detected_type = detect_type(value)
        if detected_type != 'null':  # Ignore null values in type inference
            type_counts[detected_type] += 1

    # Return most common type, default to string
    if not type_counts:
        return 'string'

    return max(type_counts, key=type_counts.get)

def infer_schema(batch, current_schema):
    """Infers and evolves schema from the batch with type detection."""
    # Schema structure: {field_name: {type: str, sample_values: list}}
    if not isinstance(current_schema, dict):
        current_schema = {}

    # Collect all values for each field in this batch
    field_values = defaultdict(list)

    for record in batch:
        for key, value in record.items():
            field_values[key].append(value)

    # Update schema with type information
    added = False
    updated = False

    for field, values in field_values.items():
        inferred_type = infer_field_type(values)

        if field not in current_schema:
            current_schema[field] = {
                'type': inferred_type,
                'sample_values': values[:3]  # Store first 3 samples
            }
            added = True
        else:
            # Update type if needed (type evolution)
            existing_type = current_schema[field]['type']
            if existing_type != inferred_type:
                # Type priority: string > float > integer
                type_priority = {'string': 3, 'email': 3, 'url': 3, 'date': 2, 'float': 2, 'integer': 1, 'boolean': 1, 'null': 0}
                if type_priority.get(inferred_type, 0) > type_priority.get(existing_type, 0):
                    current_schema[field]['type'] = inferred_type
                    updated = True

    if added:
        logging.info(f"Schema evolved: New fields added - {list(field_values.keys())}")
    if updated:
        logging.info(f"Schema updated: Types refined")

    return current_schema

def normalize_value(value, expected_type):
    """Normalize value based on expected type."""
    if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
        return None

    str_value = str(value).strip()

    try:
        if expected_type == 'integer':
            return int(float(str_value))  # Handle "42.0" -> 42
        elif expected_type == 'float':
            return float(str_value)
        elif expected_type == 'boolean':
            return str_value.lower() in ['true', 'yes', '1']
        elif expected_type == 'email':
            return str_value.lower()  # Normalize email to lowercase
        elif expected_type == 'date':
            # Try to parse and standardize date format
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y']:
                try:
                    dt = datetime.strptime(str_value, fmt)
                    return dt.strftime('%Y-%m-%d')  # Standard format
                except ValueError:
                    continue
            return str_value
        else:
            return str_value
    except (ValueError, TypeError):
        return value  # Return original if conversion fails

def clean_record(record, schema):
    """Ensures every record matches the schema, fills missing values, and normalizes data."""
    cleaned = {}

    for field, field_info in schema.items():
        value = record.get(field, None)
        expected_type = field_info.get('type', 'string')

        # Normalize the value based on expected type
        cleaned[field] = normalize_value(value, expected_type)

    return cleaned

def transform_batch(batch, schema):
    """Transforms all records in batch to match the current schema."""
    return [clean_record(rec, schema) for rec in batch]
