"""
Text File Extractor for Dynamic ETL Pipeline
Handles various .txt file formats and converts to structured data
"""

import re
import logging
from typing import List, Dict, Any

class TxtExtractor:
    """Extract structured data from .txt files"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def extract_from_txt(self, file_storage, mode='auto'):
        """
        Extract data from text file

        Args:
            file_storage: Flask file storage object
            mode: 'auto', 'key_value', 'tabular', 'line_records', 'json_lines'

        Returns:
            List of dictionaries (structured records)
        """
        # Read file content
        file_storage.stream.seek(0)
        content = file_storage.read().decode('utf-8', errors='ignore')

        if mode == 'auto':
            # Auto-detect format
            mode = self._detect_format(content)
            self.logger.info(f"Auto-detected text format: {mode}")

        # Extract based on format
        if mode == 'json_lines':
            return self._extract_json_lines(content)
        elif mode == 'key_value':
            return self._extract_key_value_pairs(content)
        elif mode == 'tabular':
            return self._extract_tabular(content)
        elif mode == 'line_records':
            return self._extract_line_records(content)
        elif mode == 'log_file':
            return self._extract_log_entries(content)
        else:
            # Fallback: try multiple methods
            return self._extract_intelligent(content)

    def _detect_format(self, content: str) -> str:
        """Auto-detect the text file format"""
        lines = content.strip().split('\n')
        if not lines:
            return 'line_records'

        first_line = lines[0].strip()

        # Check for JSON lines
        if first_line.startswith('{') and first_line.endswith('}'):
            try:
                import json
                json.loads(first_line)
                return 'json_lines'
            except:
                pass

        # Check for log file patterns
        log_patterns = [
            r'^\d{4}-\d{2}-\d{2}',  # Date prefix
            r'^\[\d{4}-\d{2}-\d{2}',  # [Date] prefix
            r'^(INFO|ERROR|WARNING|DEBUG)',  # Log level prefix
        ]
        for pattern in log_patterns:
            if re.match(pattern, first_line):
                return 'log_file'

        # Check for key:value or key=value format
        if ':' in first_line or '=' in first_line:
            kv_count = sum(1 for line in lines[:10] if ':' in line or '=' in line)
            if kv_count >= 5:  # At least half are key-value
                return 'key_value'

        # Check for tabular (tab or multiple spaces)
        if '\t' in first_line or re.search(r'\s{2,}', first_line):
            return 'tabular'

        # Default
        return 'line_records'

    def _extract_json_lines(self, content: str) -> List[Dict]:
        """Extract JSON lines format (one JSON object per line)"""
        import json
        records = []

        for i, line in enumerate(content.strip().split('\n'), 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                if isinstance(record, dict):
                    records.append(record)
                else:
                    records.append({'value': record, 'line_number': i})
            except json.JSONDecodeError as e:
                self.logger.warning(f"Invalid JSON on line {i}: {e}")
                records.append({'raw_text': line, 'line_number': i, 'error': 'invalid_json'})

        return records

    def _extract_key_value_pairs(self, content: str) -> List[Dict]:
        """
        Extract key-value pairs
        Formats:
          - key: value
          - key=value
          - key = value
        """
        records = []
        current_record = {}
        record_num = 1

        for line in content.split('\n'):
            line = line.strip()

            # Empty line = new record separator
            if not line:
                if current_record:
                    current_record['record_id'] = record_num
                    records.append(current_record)
                    current_record = {}
                    record_num += 1
                continue

            # Try to parse key-value
            match = re.match(r'^([^:=]+)[:=]\s*(.+)$', line)
            if match:
                key = match.group(1).strip()
                value = match.group(2).strip()

                # Clean key (remove special chars, make snake_case)
                key = re.sub(r'[^\w\s]', '', key)
                key = re.sub(r'\s+', '_', key).lower()

                # Try to convert value to appropriate type
                value = self._convert_value(value)

                current_record[key] = value
            else:
                # Can't parse as key-value, add as text
                if 'additional_text' not in current_record:
                    current_record['additional_text'] = []
                current_record['additional_text'].append(line)

        # Add last record
        if current_record:
            current_record['record_id'] = record_num
            records.append(current_record)

        return records

    def _extract_tabular(self, content: str) -> List[Dict]:
        """Extract tab-separated or space-separated tabular data"""
        lines = content.strip().split('\n')
        if not lines:
            return []

        # Detect delimiter
        first_line = lines[0]
        if '\t' in first_line:
            delimiter = '\t'
        else:
            delimiter = r'\s{2,}'  # Multiple spaces

        # First line might be headers
        if delimiter == '\t':
            headers = first_line.split('\t')
        else:
            headers = re.split(delimiter, first_line)

        headers = [h.strip().lower().replace(' ', '_') for h in headers]

        # Check if first line looks like headers (not all numeric)
        first_line_values = headers
        is_header = not all(self._is_numeric(v) for v in first_line_values if v)

        records = []
        start_idx = 1 if is_header else 0

        if not is_header:
            # Generate headers
            headers = [f'field_{i+1}' for i in range(len(headers))]

        for i, line in enumerate(lines[start_idx:], start_idx):
            line = line.strip()
            if not line:
                continue

            if delimiter == '\t':
                values = line.split('\t')
            else:
                values = re.split(delimiter, line)

            values = [v.strip() for v in values]

            # Create record
            record = {}
            for j, header in enumerate(headers):
                if j < len(values):
                    record[header] = self._convert_value(values[j])
                else:
                    record[header] = None

            record['line_number'] = i
            records.append(record)

        return records

    def _extract_line_records(self, content: str) -> List[Dict]:
        """Each line becomes a record"""
        records = []

        for i, line in enumerate(content.split('\n'), 1):
            line = line.strip()
            if not line:
                continue

            # Try to extract structured info from the line
            record = {
                'line_number': i,
                'text': line
            }

            # Extract common patterns
            # Emails
            emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', line)
            if emails:
                record['emails'] = emails

            # Phone numbers
            phones = re.findall(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', line)
            if phones:
                record['phones'] = phones

            # Dates
            dates = re.findall(r'\b\d{4}-\d{2}-\d{2}\b|\b\d{2}/\d{2}/\d{4}\b', line)
            if dates:
                record['dates'] = dates

            # Numbers (amounts, IDs, etc.)
            numbers = re.findall(r'\b\d+\.?\d*\b', line)
            if numbers:
                record['numbers'] = [self._convert_value(n) for n in numbers]

            records.append(record)

        return records

    def _extract_log_entries(self, content: str) -> List[Dict]:
        """Extract log file entries"""
        records = []

        # Common log patterns
        patterns = [
            # Apache/Nginx style: [date] level message
            r'^\[(?P<timestamp>[^\]]+)\]\s+(?P<level>\w+):\s+(?P<message>.+)$',
            # Python logging: level:name:message
            r'^(?P<level>INFO|ERROR|WARNING|DEBUG):(?P<name>[^:]+):(?P<message>.+)$',
            # Simple: timestamp level message
            r'^(?P<timestamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+(?P<level>\w+)\s+(?P<message>.+)$',
        ]

        for i, line in enumerate(content.split('\n'), 1):
            line = line.strip()
            if not line:
                continue

            record = {'line_number': i}
            matched = False

            for pattern in patterns:
                match = re.match(pattern, line)
                if match:
                    record.update(match.groupdict())
                    matched = True
                    break

            if not matched:
                record['raw_log'] = line

            records.append(record)

        return records

    def _extract_intelligent(self, content: str) -> List[Dict]:
        """Intelligent extraction using multiple methods"""
        # Try JSON lines first
        try:
            records = self._extract_json_lines(content)
            if records and all('error' not in r for r in records):
                return records
        except:
            pass

        # Try key-value
        try:
            records = self._extract_key_value_pairs(content)
            if records and len(records) > 0:
                # Check if we got meaningful data
                if any(len(r) > 2 for r in records):  # At least some records with >2 fields
                    return records
        except:
            pass

        # Fallback to line records
        return self._extract_line_records(content)

    def _convert_value(self, value: str) -> Any:
        """Convert string value to appropriate type"""
        if not value:
            return None

        value = value.strip()

        # Try boolean
        if value.lower() in ('true', 'yes', 'y'):
            return True
        if value.lower() in ('false', 'no', 'n'):
            return False

        # Try integer
        if self._is_integer(value):
            return int(value)

        # Try float
        if self._is_numeric(value):
            return float(value)

        # Return as string
        return value

    def _is_integer(self, value: str) -> bool:
        """Check if string is an integer"""
        try:
            int(value)
            return '.' not in value
        except:
            return False

    def _is_numeric(self, value: str) -> bool:
        """Check if string is numeric"""
        try:
            float(value)
            return True
        except:
            return False


def extract_data_from_txt(file_storage, mode='auto'):
    """
    Main function to extract data from .txt files

    Args:
        file_storage: Flask file storage object
        mode: Extraction mode ('auto', 'key_value', 'tabular', 'line_records', 'json_lines', 'log_file')

    Returns:
        List of dictionaries (structured records)
    """
    extractor = TxtExtractor()
    return extractor.extract_from_txt(file_storage, mode=mode)
