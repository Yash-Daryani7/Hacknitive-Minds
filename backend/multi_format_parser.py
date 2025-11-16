"""
Multi-Format Parser for Mixed Content Files
Extracts JSON, HTML tables, CSV, key-value pairs, YAML, and more from single files
"""

import re
import json
import logging
from collections import defaultdict
from datetime import datetime
import io

try:
    from bs4 import BeautifulSoup
    import pandas as pd
    import yaml
    PARSING_AVAILABLE = True
except ImportError:
    PARSING_AVAILABLE = False
    logging.warning("Parsing dependencies not available")


class FragmentExtractor:
    """Extract different data formats from mixed content"""

    def __init__(self):
        self.fragments = []
        self.content = ""
        self.offsets = []

    def extract_all(self, content, source_type='txt'):
        """
        Extract all fragments from content
        Returns structured fragments with offsets
        """
        self.content = content
        self.fragments = []

        # Extract different fragment types
        json_fragments = self._extract_json_fragments()
        html_fragments = self._extract_html_fragments()
        csv_fragments = self._extract_csv_fragments()
        kv_fragments = self._extract_key_value_pairs()
        yaml_fragments = self._extract_yaml_fragments()
        table_fragments = self._extract_html_tables()
        sql_fragments = self._extract_sql_snippets()
        jsonld_fragments = self._extract_jsonld()

        # Combine all fragments
        all_fragments = {
            'json_fragments': json_fragments,
            'html_tables': table_fragments,
            'csv_sections': csv_fragments,
            'kv_pairs': kv_fragments,
            'yaml_sections': yaml_fragments,
            'html_content': html_fragments,
            'sql_snippets': sql_fragments,
            'jsonld': jsonld_fragments
        }

        # Generate summary
        summary = {
            'total_fragments': sum(len(v) for v in all_fragments.values()),
            'json_fragments': len(json_fragments),
            'html_tables': len(table_fragments),
            'csv_sections': len(csv_fragments),
            'kv_pairs': len(kv_fragments),
            'yaml_sections': len(yaml_fragments),
            'html_snippets': len(html_fragments),
            'sql_snippets': len(sql_fragments),
            'jsonld_blocks': len(jsonld_fragments)
        }

        return all_fragments, summary

    def _extract_json_fragments(self):
        """Extract JSON objects and arrays, including malformed ones"""
        fragments = []

        # Pattern for JSON objects
        json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'

        for match in re.finditer(json_pattern, self.content, re.DOTALL):
            start, end = match.span()
            json_text = match.group(0)

            try:
                # Try to parse as valid JSON
                data = json.loads(json_text)
                fragments.append({
                    'type': 'json',
                    'status': 'valid',
                    'data': data,
                    'offset': {'start': start, 'end': end},
                    'raw': json_text[:200]  # First 200 chars
                })
            except json.JSONDecodeError:
                # Try to fix common issues
                fixed_json = self._attempt_json_fix(json_text)
                if fixed_json:
                    fragments.append({
                        'type': 'json',
                        'status': 'repaired',
                        'data': fixed_json,
                        'offset': {'start': start, 'end': end},
                        'raw': json_text[:200]
                    })
                else:
                    fragments.append({
                        'type': 'json',
                        'status': 'malformed',
                        'data': None,
                        'offset': {'start': start, 'end': end},
                        'raw': json_text[:200],
                        'error': 'Could not parse JSON'
                    })

        return fragments

    def _attempt_json_fix(self, json_text):
        """Attempt to fix common JSON errors"""
        try:
            # Remove trailing commas
            fixed = re.sub(r',(\s*[}\]])', r'\1', json_text)
            # Add missing quotes to keys
            fixed = re.sub(r'(\w+):', r'"\1":', fixed)
            # Try parsing
            return json.loads(fixed)
        except:
            return None

    def _extract_html_tables(self):
        """Extract HTML tables as structured data"""
        if not PARSING_AVAILABLE:
            return []

        fragments = []
        soup = BeautifulSoup(self.content, 'html.parser')

        for idx, table in enumerate(soup.find_all('table')):
            # Find table position in original content
            table_str = str(table)
            start = self.content.find(table_str)
            end = start + len(table_str) if start != -1 else -1

            # Extract table data
            rows = []
            headers = []

            # Get headers
            header_row = table.find('thead')
            if header_row:
                headers = [th.get_text(strip=True) for th in header_row.find_all('th')]
            elif table.find('tr'):
                first_row = table.find('tr')
                headers = [th.get_text(strip=True) for th in first_row.find_all(['th', 'td'])]

            # Get data rows
            tbody = table.find('tbody') or table
            for row in tbody.find_all('tr'):
                cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
                if len(cells) == len(headers) and cells:
                    row_data = dict(zip(headers, cells))
                    rows.append(row_data)

            fragments.append({
                'type': 'html_table',
                'table_index': idx,
                'headers': headers,
                'rows': rows,
                'row_count': len(rows),
                'col_count': len(headers),
                'offset': {'start': start, 'end': end}
            })

        return fragments

    def _extract_csv_fragments(self):
        """Extract CSV-like sections"""
        fragments = []

        # Pattern for CSV sections (multiple lines with consistent delimiters)
        lines = self.content.split('\n')

        csv_section = []
        section_start = 0
        delimiter = None

        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                if csv_section and len(csv_section) > 1:
                    # Process accumulated CSV section
                    fragment = self._parse_csv_section(csv_section, section_start, delimiter)
                    if fragment:
                        fragments.append(fragment)
                csv_section = []
                delimiter = None
                continue

            # Detect delimiter
            detected_delim = self._detect_csv_delimiter(line)
            if detected_delim:
                if delimiter is None:
                    delimiter = detected_delim
                    section_start = self.content.find(line)

                if delimiter == detected_delim:
                    csv_section.append(line)
                else:
                    # Delimiter changed, process previous section
                    if csv_section and len(csv_section) > 1:
                        fragment = self._parse_csv_section(csv_section, section_start, delimiter)
                        if fragment:
                            fragments.append(fragment)
                    csv_section = [line]
                    delimiter = detected_delim
                    section_start = self.content.find(line)

        # Process remaining section
        if csv_section and len(csv_section) > 1:
            fragment = self._parse_csv_section(csv_section, section_start, delimiter)
            if fragment:
                fragments.append(fragment)

        return fragments

    def _detect_csv_delimiter(self, line):
        """Detect CSV delimiter"""
        delimiters = [',', '\t', ';', '|']
        counts = {d: line.count(d) for d in delimiters}

        # Must have at least 1 delimiter
        valid_delims = {d: c for d, c in counts.items() if c > 0}
        if not valid_delims:
            return None

        # Return most common delimiter
        return max(valid_delims, key=valid_delims.get)

    def _parse_csv_section(self, lines, start_offset, delimiter):
        """Parse CSV section into structured data"""
        if not PARSING_AVAILABLE or not lines:
            return None

        try:
            csv_text = '\n'.join(lines)
            df = pd.read_csv(io.StringIO(csv_text), delimiter=delimiter)

            end_offset = start_offset + len(csv_text)

            return {
                'type': 'csv',
                'delimiter': delimiter,
                'headers': df.columns.tolist(),
                'rows': df.to_dict('records'),
                'row_count': len(df),
                'col_count': len(df.columns),
                'offset': {'start': start_offset, 'end': end_offset}
            }
        except:
            return None

    def _extract_key_value_pairs(self):
        """Extract key-value pairs from text"""
        fragments = []

        # Pattern for key: value pairs
        kv_pattern = r'^([a-zA-Z_][\w\s]*?):\s*(.+?)$'

        kv_block = []
        block_start = None

        for match in re.finditer(kv_pattern, self.content, re.MULTILINE):
            start, end = match.span()
            key = match.group(1).strip()
            value = match.group(2).strip()

            if block_start is None:
                block_start = start

            kv_block.append({'key': key, 'value': value})

            # Check if next line is also KV
            next_line_start = self.content.find('\n', end)
            if next_line_start != -1:
                next_line = self.content[next_line_start:next_line_start+100]
                if not re.match(kv_pattern, next_line.strip()):
                    # End of KV block
                    if len(kv_block) >= 2:  # At least 2 KV pairs
                        fragments.append({
                            'type': 'key_value',
                            'pairs': kv_block,
                            'count': len(kv_block),
                            'offset': {'start': block_start, 'end': end}
                        })
                    kv_block = []
                    block_start = None

        # Add remaining block
        if len(kv_block) >= 2:
            fragments.append({
                'type': 'key_value',
                'pairs': kv_block,
                'count': len(kv_block),
                'offset': {'start': block_start, 'end': self.content.rfind(kv_block[-1]['value'])}
            })

        return fragments

    def _extract_yaml_fragments(self):
        """Extract YAML/frontmatter sections"""
        if not PARSING_AVAILABLE:
            return []

        fragments = []

        # Pattern for YAML frontmatter (--- ... ---)
        yaml_pattern = r'^---\n(.*?)\n---'

        for match in re.finditer(yaml_pattern, self.content, re.DOTALL | re.MULTILINE):
            start, end = match.span()
            yaml_text = match.group(1)

            try:
                data = yaml.safe_load(yaml_text)
                fragments.append({
                    'type': 'yaml',
                    'status': 'valid',
                    'data': data,
                    'offset': {'start': start, 'end': end},
                    'raw': yaml_text[:200]
                })
            except:
                fragments.append({
                    'type': 'yaml',
                    'status': 'malformed',
                    'data': None,
                    'offset': {'start': start, 'end': end},
                    'raw': yaml_text[:200]
                })

        return fragments

    def _extract_html_fragments(self):
        """Extract HTML snippets (non-table)"""
        if not PARSING_AVAILABLE:
            return []

        fragments = []
        soup = BeautifulSoup(self.content, 'html.parser')

        # Extract divs, spans with meaningful content
        for tag in soup.find_all(['div', 'section', 'article']):
            tag_str = str(tag)
            start = self.content.find(tag_str)
            end = start + len(tag_str) if start != -1 else -1

            # Skip if it's a table (already extracted)
            if tag.find('table'):
                continue

            text = tag.get_text(strip=True)
            if len(text) > 20:  # Meaningful content
                fragments.append({
                    'type': 'html_content',
                    'tag': tag.name,
                    'text': text[:200],
                    'offset': {'start': start, 'end': end}
                })

        return fragments[:10]  # Limit to 10 HTML snippets

    def _extract_sql_snippets(self):
        """Extract SQL code blocks (without executing)"""
        fragments = []

        # Pattern for SQL SELECT statements
        sql_pattern = r'(SELECT\s+.+?FROM\s+.+?(?:WHERE|GROUP|ORDER|LIMIT|;|\n\n))'

        for match in re.finditer(sql_pattern, self.content, re.IGNORECASE | re.DOTALL):
            start, end = match.span()
            sql_text = match.group(1).strip()

            fragments.append({
                'type': 'sql_snippet',
                'sql': sql_text,
                'offset': {'start': start, 'end': end},
                'note': 'NOT EXECUTED - extracted as text only'
            })

        return fragments

    def _extract_jsonld(self):
        """Extract JSON-LD schema.org blocks"""
        fragments = []

        # Pattern for <script type="application/ld+json">
        jsonld_pattern = r'<script\s+type="application/ld\+json">(.*?)</script>'

        for match in re.finditer(jsonld_pattern, self.content, re.DOTALL | re.IGNORECASE):
            start, end = match.span()
            json_text = match.group(1).strip()

            try:
                data = json.loads(json_text)
                fragments.append({
                    'type': 'jsonld',
                    'status': 'valid',
                    'data': data,
                    'schema_type': data.get('@type', 'Unknown'),
                    'offset': {'start': start, 'end': end}
                })
            except:
                fragments.append({
                    'type': 'jsonld',
                    'status': 'malformed',
                    'data': None,
                    'offset': {'start': start, 'end': end}
                })

        return fragments


class UnifiedRecordGenerator:
    """Generate unified records from extracted fragments"""

    def __init__(self):
        self.fragment_extractor = FragmentExtractor()

    def generate_records(self, content, source_id, file_id):
        """
        Generate unified records from all fragments
        Returns records list and metadata
        """
        all_fragments, summary = self.fragment_extractor.extract_all(content)

        records = []
        record_id = 0

        # Process each fragment type
        for json_frag in all_fragments.get('json_fragments', []):
            if json_frag['status'] in ['valid', 'repaired'] and json_frag['data']:
                record = self._flatten_json(json_frag['data'])
                record['_fragment_type'] = 'json'
                record['_fragment_id'] = record_id
                record['_source_offset'] = json_frag['offset']
                record['_source_id'] = source_id
                record['_file_id'] = file_id
                records.append(record)
                record_id += 1

        # HTML tables
        for table_frag in all_fragments.get('html_tables', []):
            for row in table_frag['rows']:
                row['_fragment_type'] = 'html_table'
                row['_fragment_id'] = record_id
                row['_table_index'] = table_frag['table_index']
                row['_source_offset'] = table_frag['offset']
                row['_source_id'] = source_id
                row['_file_id'] = file_id
                records.append(row)
                record_id += 1

        # CSV sections
        for csv_frag in all_fragments.get('csv_sections', []):
            for row in csv_frag['rows']:
                row['_fragment_type'] = 'csv'
                row['_fragment_id'] = record_id
                row['_source_offset'] = csv_frag['offset']
                row['_source_id'] = source_id
                row['_file_id'] = file_id
                records.append(row)
                record_id += 1

        # Key-value pairs
        for kv_frag in all_fragments.get('kv_pairs', []):
            record = {pair['key']: pair['value'] for pair in kv_frag['pairs']}
            record['_fragment_type'] = 'key_value'
            record['_fragment_id'] = record_id
            record['_source_offset'] = kv_frag['offset']
            record['_source_id'] = source_id
            record['_file_id'] = file_id
            records.append(record)
            record_id += 1

        # YAML sections
        for yaml_frag in all_fragments.get('yaml_sections', []):
            if yaml_frag['status'] == 'valid' and yaml_frag['data']:
                record = self._flatten_json(yaml_frag['data'])
                record['_fragment_type'] = 'yaml'
                record['_fragment_id'] = record_id
                record['_source_offset'] = yaml_frag['offset']
                record['_source_id'] = source_id
                record['_file_id'] = file_id
                records.append(record)
                record_id += 1

        # JSON-LD
        for jsonld_frag in all_fragments.get('jsonld', []):
            if jsonld_frag['status'] == 'valid' and jsonld_frag['data']:
                record = self._flatten_json(jsonld_frag['data'])
                record['_fragment_type'] = 'jsonld'
                record['_fragment_id'] = record_id
                record['_schema_type'] = jsonld_frag.get('schema_type')
                record['_source_offset'] = jsonld_frag['offset']
                record['_source_id'] = source_id
                record['_file_id'] = file_id
                records.append(record)
                record_id += 1

        metadata = {
            'source_id': source_id,
            'file_id': file_id,
            'total_records': len(records),
            'fragments_summary': summary,
            'extracted_at': datetime.now().isoformat()
        }

        return records, metadata

    def _flatten_json(self, data, parent_key='', sep='.'):
        """Flatten nested JSON"""
        items = []

        if isinstance(data, dict):
            for k, v in data.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, (dict, list)):
                    items.extend(self._flatten_json(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_key = f"{parent_key}[{i}]"
                if isinstance(item, (dict, list)):
                    items.extend(self._flatten_json(item, new_key, sep=sep).items())
                else:
                    items.append((new_key, item))
        else:
            items.append((parent_key, data))

        return dict(items)


# Global instance
unified_record_generator = UnifiedRecordGenerator()
