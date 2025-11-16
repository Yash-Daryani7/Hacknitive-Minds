import json
import csv
import logging

def extract_data(file_storage):
    """Extract data from uploaded file (JSON, CSV, PDF, or TXT)."""
    filename = file_storage.filename

    if filename.endswith(".json"):
        data = json.load(file_storage)
    elif filename.endswith(".csv"):
        file_storage.stream.seek(0)
        reader = csv.DictReader(file_storage.stream.read().decode("utf-8").splitlines())
        data = list(reader)
    elif filename.endswith(".pdf"):
        # Import PDF extractor
        try:
            from pdf_extractor import extract_data_from_pdf
            data = extract_data_from_pdf(file_storage, mode='auto')
            logging.info(f"Extracted {len(data)} records from PDF")
        except ImportError:
            logging.error("PDF extractor not available. Install PDF dependencies.")
            data = []
        except Exception as e:
            logging.error(f"PDF extraction failed: {e}")
            data = []
    elif filename.endswith(".txt"):
        # Import TXT extractor
        try:
            from txt_extractor import extract_data_from_txt
            data = extract_data_from_txt(file_storage, mode='auto')
            logging.info(f"Extracted {len(data)} records from TXT file")
        except ImportError:
            logging.error("TXT extractor not available.")
            data = []
        except Exception as e:
            logging.error(f"TXT extraction failed: {e}")
            data = []
    else:
        # Unsupported type
        logging.warning(f"Unsupported file type: {filename}")
        data = []

    return data

def batch_data(data, batch_size):
    """Yield data in batches for scalability."""
    batch = []
    for record in data:
        batch.append(record)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch