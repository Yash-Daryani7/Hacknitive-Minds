"""
PDF Data Extraction Module
Extracts text, tables, and structured data from PDF files
"""

import logging
import re
from collections import defaultdict
from datetime import datetime

try:
    import PyPDF2
    import pdfplumber
    import pandas as pd
    import tabula
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    logging.warning("PDF dependencies not installed. Install: pip install PyPDF2 pdfplumber tabula-py")


class PDFExtractor:
    """Extract data from PDF files"""

    def __init__(self):
        if not PDF_AVAILABLE:
            logging.error("PDF libraries not available")

    def extract_text(self, pdf_path):
        """
        Extract all text from PDF
        Returns list of pages with text
        """
        if not PDF_AVAILABLE:
            return []

        try:
            text_data = []

            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    text = page.extract_text()

                    if text:
                        text_data.append({
                            'page': page_num,
                            'text': text.strip(),
                            'char_count': len(text),
                            'line_count': len(text.split('\n'))
                        })

            logging.info(f"Extracted text from {len(text_data)} pages")
            return text_data

        except Exception as e:
            logging.error(f"PDF text extraction failed: {e}")
            return []

    def extract_tables(self, pdf_path):
        """
        Extract tables from PDF
        Returns list of DataFrames (one per table)
        """
        if not PDF_AVAILABLE:
            return []

        all_tables = []

        try:
            # Method 1: Using pdfplumber
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    tables = page.extract_tables()

                    for table_idx, table in enumerate(tables):
                        if table and len(table) > 0:
                            # Convert to DataFrame
                            df = pd.DataFrame(table[1:], columns=table[0])

                            # Clean column names
                            df.columns = [str(col).strip() if col else f'Column_{i}' for i, col in enumerate(df.columns)]

                            # Add metadata
                            table_data = {
                                'page': page_num,
                                'table_index': table_idx,
                                'rows': len(df),
                                'columns': len(df.columns),
                                'data': df.to_dict('records')
                            }

                            all_tables.append(table_data)

            logging.info(f"Extracted {len(all_tables)} tables from PDF")
            return all_tables

        except Exception as e:
            logging.warning(f"pdfplumber table extraction failed: {e}")

            # Fallback: Try tabula-py
            try:
                import tabula
                tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True)

                for idx, df in enumerate(tables):
                    if not df.empty:
                        table_data = {
                            'page': 'unknown',
                            'table_index': idx,
                            'rows': len(df),
                            'columns': len(df.columns),
                            'data': df.to_dict('records')
                        }
                        all_tables.append(table_data)

                logging.info(f"Extracted {len(all_tables)} tables using tabula")
                return all_tables

            except Exception as e2:
                logging.error(f"Tabula table extraction also failed: {e2}")
                return []

    def extract_structured_data(self, pdf_path, patterns=None):
        """
        Extract structured data using regex patterns
        Useful for forms, invoices, receipts

        patterns: dict of {field_name: regex_pattern}
        """
        if not PDF_AVAILABLE:
            return []

        if patterns is None:
            # Default patterns for common fields
            patterns = {
                'email': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
                'phone': r'\+?[\d\s\-\(\)]{10,}',
                'date': r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',
                'amount': r'\$?\s*\d+[,\.]?\d*',
                'url': r'https?://[^\s]+',
            }

        try:
            extracted_data = []

            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    text = page.extract_text()

                    if not text:
                        continue

                    page_data = {
                        'page': page_num,
                        'extracted_fields': {}
                    }

                    # Apply each pattern
                    for field_name, pattern in patterns.items():
                        matches = re.findall(pattern, text)

                        if matches:
                            # Remove duplicates while preserving order
                            unique_matches = list(dict.fromkeys(matches))
                            page_data['extracted_fields'][field_name] = unique_matches

                    if page_data['extracted_fields']:
                        extracted_data.append(page_data)

            logging.info(f"Extracted structured data from {len(extracted_data)} pages")
            return extracted_data

        except Exception as e:
            logging.error(f"Structured data extraction failed: {e}")
            return []

    def extract_metadata(self, pdf_path):
        """
        Extract PDF metadata
        """
        if not PDF_AVAILABLE:
            return {}

        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)

                metadata = {
                    'pages': len(pdf_reader.pages),
                    'title': pdf_reader.metadata.get('/Title', 'Unknown'),
                    'author': pdf_reader.metadata.get('/Author', 'Unknown'),
                    'subject': pdf_reader.metadata.get('/Subject', 'Unknown'),
                    'creator': pdf_reader.metadata.get('/Creator', 'Unknown'),
                    'producer': pdf_reader.metadata.get('/Producer', 'Unknown'),
                    'creation_date': pdf_reader.metadata.get('/CreationDate', 'Unknown'),
                }

                return metadata

        except Exception as e:
            logging.error(f"Metadata extraction failed: {e}")
            return {}

    def extract_all(self, pdf_path, extract_tables=True, extract_patterns=None):
        """
        Extract everything from PDF
        Returns comprehensive data structure
        """
        result = {
            'metadata': self.extract_metadata(pdf_path),
            'text_pages': self.extract_text(pdf_path),
            'tables': [],
            'structured_data': [],
            'extraction_timestamp': datetime.now().isoformat()
        }

        if extract_tables:
            result['tables'] = self.extract_tables(pdf_path)

        if extract_patterns:
            result['structured_data'] = self.extract_structured_data(pdf_path, extract_patterns)

        return result


class PDFToDataConverter:
    """Convert PDF extractions to standard data format"""

    def __init__(self):
        self.extractor = PDFExtractor()

    def pdf_to_records(self, pdf_path, mode='tables'):
        """
        Convert PDF to list of records (dicts)

        Modes:
        - 'tables': Extract tables and convert to records
        - 'text': Parse text into records (custom logic needed)
        - 'structured': Use patterns to extract structured data
        - 'auto': Try all methods and return best result
        """

        if mode == 'tables' or mode == 'auto':
            # Extract tables
            tables = self.extractor.extract_tables(pdf_path)

            if tables:
                # Combine all table data
                all_records = []
                for table in tables:
                    records = table.get('data', [])
                    # Add metadata to each record
                    for record in records:
                        record['_pdf_page'] = table.get('page')
                        record['_pdf_table'] = table.get('table_index')
                    all_records.extend(records)

                return all_records

        if mode == 'structured' or (mode == 'auto' and not tables):
            # Extract structured data
            structured = self.extractor.extract_structured_data(pdf_path)

            if structured:
                records = []
                for page_data in structured:
                    record = {
                        '_pdf_page': page_data.get('page'),
                        **page_data.get('extracted_fields', {})
                    }
                    records.append(record)

                return records

        if mode == 'text' or (mode == 'auto' and not tables and not structured):
            # Extract text and parse
            text_pages = self.extractor.extract_text(pdf_path)

            records = []
            for page in text_pages:
                record = {
                    '_pdf_page': page.get('page'),
                    'content': page.get('text'),
                    'char_count': page.get('char_count'),
                    'line_count': page.get('line_count')
                }
                records.append(record)

            return records

        return []

    def invoice_pdf_to_records(self, pdf_path):
        """
        Specialized extraction for invoice PDFs
        """
        patterns = {
            'invoice_number': r'Invoice\s*#?\s*:?\s*(\w+)',
            'date': r'Date\s*:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
            'total': r'Total\s*:?\s*\$?\s*([\d,]+\.?\d*)',
            'email': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
            'phone': r'\+?[\d\s\-\(\)]{10,}',
        }

        structured = self.extractor.extract_structured_data(pdf_path, patterns)
        tables = self.extractor.extract_tables(pdf_path)

        # Combine invoice data
        invoice_data = {
            '_pdf_type': 'invoice',
            'metadata': {},
            'line_items': []
        }

        # Extract metadata from structured data
        for page_data in structured:
            invoice_data['metadata'].update(page_data.get('extracted_fields', {}))

        # Extract line items from tables
        if tables:
            for table in tables:
                invoice_data['line_items'].extend(table.get('data', []))

        return [invoice_data]

    def resume_pdf_to_records(self, pdf_path):
        """
        Specialized extraction for resume PDFs
        """
        patterns = {
            'name': r'^[A-Z][a-z]+\s+[A-Z][a-z]+',
            'email': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
            'phone': r'\+?[\d\s\-\(\)]{10,}',
            'linkedin': r'linkedin\.com/in/[\w\-]+',
            'github': r'github\.com/[\w\-]+',
        }

        text_pages = self.extractor.extract_text(pdf_path)
        structured = self.extractor.extract_structured_data(pdf_path, patterns)

        resume_data = {
            '_pdf_type': 'resume',
            'full_text': '\n\n'.join([p['text'] for p in text_pages]),
            'extracted_info': {}
        }

        # Extract structured info
        for page_data in structured:
            resume_data['extracted_info'].update(page_data.get('extracted_fields', {}))

        return [resume_data]


# Global instances
pdf_extractor = PDFExtractor()
pdf_converter = PDFToDataConverter()


def extract_data_from_pdf(pdf_file_storage, mode='auto'):
    """
    Main function to extract data from uploaded PDF
    Compatible with Flask file upload

    pdf_file_storage: Flask FileStorage object
    mode: 'tables', 'text', 'structured', 'auto'
    """
    # Save uploaded file temporarily
    import tempfile
    import os

    temp_dir = tempfile.gettempdir()
    temp_path = os.path.join(temp_dir, pdf_file_storage.filename)

    # Save file
    pdf_file_storage.save(temp_path)

    try:
        # Extract data
        records = pdf_converter.pdf_to_records(temp_path, mode=mode)

        # Clean up
        os.remove(temp_path)

        return records

    except Exception as e:
        logging.error(f"PDF processing failed: {e}")
        # Clean up on error
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return []
