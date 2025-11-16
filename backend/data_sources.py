"""
Multiple Data Sources Support
Web Scraping, API Integration, and Multi-source Data Ingestion
"""

import logging
import json
import csv
from io import StringIO
from datetime import datetime

try:
    import requests
    from bs4 import BeautifulSoup
    import lxml
    SCRAPING_AVAILABLE = True
except ImportError:
    SCRAPING_AVAILABLE = False
    logging.warning("Web scraping dependencies not available")


class WebScraper:
    """Web scraping functionality using BeautifulSoup"""

    def __init__(self):
        self.session = requests.Session() if SCRAPING_AVAILABLE else None
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }) if SCRAPING_AVAILABLE else None

    def scrape_table(self, url, table_selector=None):
        """
        Scrape HTML table from a webpage
        Returns list of dictionaries
        """
        if not SCRAPING_AVAILABLE:
            logging.error("Web scraping not available. Install requirements.")
            return []

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')

            # Find table
            if table_selector:
                table = soup.select_one(table_selector)
            else:
                table = soup.find('table')

            if not table:
                logging.error("No table found on page")
                return []

            # Extract headers
            headers = []
            header_row = table.find('thead')
            if header_row:
                headers = [th.get_text(strip=True) for th in header_row.find_all('th')]
            else:
                # Try first row
                first_row = table.find('tr')
                if first_row:
                    headers = [th.get_text(strip=True) for th in first_row.find_all(['th', 'td'])]

            # Extract rows
            data = []
            tbody = table.find('tbody') or table
            rows = tbody.find_all('tr')

            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) == len(headers):
                    row_data = {headers[i]: cell.get_text(strip=True) for i, cell in enumerate(cells)}
                    data.append(row_data)

            logging.info(f"Scraped {len(data)} records from {url}")
            return data

        except Exception as e:
            logging.error(f"Web scraping failed: {e}")
            return []

    def scrape_custom(self, url, selectors):
        """
        Scrape custom data using CSS selectors
        selectors: dict mapping field names to CSS selectors
        """
        if not SCRAPING_AVAILABLE:
            logging.error("Web scraping not available")
            return []

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')

            data = []
            # Assuming repeating structure
            # Find all container elements for each record
            container_selector = selectors.get('_container', 'div.item')
            containers = soup.select(container_selector)

            for container in containers:
                record = {}
                for field, selector in selectors.items():
                    if field == '_container':
                        continue

                    element = container.select_one(selector)
                    if element:
                        record[field] = element.get_text(strip=True)
                    else:
                        record[field] = None

                if record:
                    data.append(record)

            logging.info(f"Scraped {len(data)} records with custom selectors")
            return data

        except Exception as e:
            logging.error(f"Custom scraping failed: {e}")
            return []

    def scrape_json_api(self, url, json_path=None):
        """
        Scrape JSON data from API endpoint
        json_path: optional dot-notation path to extract nested data
        """
        if not SCRAPING_AVAILABLE:
            logging.error("API scraping not available")
            return []

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()

            # Navigate to nested data if path provided
            if json_path:
                for key in json_path.split('.'):
                    if isinstance(data, dict):
                        data = data.get(key, [])
                    elif isinstance(data, list) and key.isdigit():
                        data = data[int(key)]
                    else:
                        break

            # Ensure data is a list
            if not isinstance(data, list):
                data = [data]

            logging.info(f"Fetched {len(data)} records from API")
            return data

        except Exception as e:
            logging.error(f"API scraping failed: {e}")
            return []


class APIConnector:
    """Connect to various REST APIs"""

    def __init__(self, api_key=None):
        self.api_key = api_key
        self.session = requests.Session() if SCRAPING_AVAILABLE else None

    def fetch_from_api(self, url, method='GET', headers=None, params=None, data=None):
        """
        Fetch data from REST API
        """
        if not SCRAPING_AVAILABLE:
            logging.error("API connector not available")
            return []

        try:
            # Add API key to headers if provided
            if headers is None:
                headers = {}

            if self.api_key:
                headers['Authorization'] = f'Bearer {self.api_key}'

            # Make request
            if method.upper() == 'GET':
                response = self.session.get(url, headers=headers, params=params, timeout=10)
            elif method.upper() == 'POST':
                response = self.session.post(url, headers=headers, json=data, timeout=10)
            else:
                logging.error(f"Unsupported HTTP method: {method}")
                return []

            response.raise_for_status()

            # Parse response
            content_type = response.headers.get('Content-Type', '')

            if 'application/json' in content_type:
                result = response.json()
                # Handle common API response patterns
                if isinstance(result, dict):
                    # Check for common data keys
                    for key in ['data', 'results', 'items', 'records']:
                        if key in result and isinstance(result[key], list):
                            return result[key]
                    # If no common key, wrap in list
                    return [result]
                elif isinstance(result, list):
                    return result
            else:
                # Try to parse as JSON anyway
                try:
                    return response.json()
                except:
                    logging.warning("Response is not JSON, returning as text")
                    return [{'content': response.text}]

            return []

        except Exception as e:
            logging.error(f"API fetch failed: {e}")
            return []

    def paginated_fetch(self, url, max_pages=10, page_param='page'):
        """
        Fetch paginated API data
        """
        all_data = []

        for page in range(1, max_pages + 1):
            params = {page_param: page}
            data = self.fetch_from_api(url, params=params)

            if not data:
                break

            all_data.extend(data)

            # If returned less than expected, probably last page
            if len(data) < 100:  # Assuming typical page size
                break

        logging.info(f"Fetched {len(all_data)} records across {page} pages")
        return all_data


class MultiSourceIngestion:
    """Unified ingestion from multiple sources"""

    def __init__(self):
        self.scraper = WebScraper()
        self.api_connector = APIConnector()

    def ingest_from_source(self, source_config):
        """
        Ingest data from any source based on config
        source_config: dict with type, url, and other params
        """
        source_type = source_config.get('type', 'unknown')

        if source_type == 'file':
            return self._ingest_file(source_config)
        elif source_type == 'url_json':
            return self._ingest_url_json(source_config)
        elif source_type == 'url_table':
            return self._ingest_url_table(source_config)
        elif source_type == 'api':
            return self._ingest_api(source_config)
        elif source_type == 'custom_scrape':
            return self._ingest_custom_scrape(source_config)
        else:
            logging.error(f"Unknown source type: {source_type}")
            return []

    def _ingest_file(self, config):
        """Ingest from uploaded file"""
        file = config.get('file')
        return extract_data_from_file(file)

    def _ingest_url_json(self, config):
        """Ingest JSON from URL"""
        url = config.get('url')
        json_path = config.get('json_path')
        return self.scraper.scrape_json_api(url, json_path)

    def _ingest_url_table(self, config):
        """Ingest table from HTML page"""
        url = config.get('url')
        selector = config.get('table_selector')
        return self.scraper.scrape_table(url, selector)

    def _ingest_api(self, config):
        """Ingest from REST API"""
        url = config.get('url')
        method = config.get('method', 'GET')
        headers = config.get('headers')
        params = config.get('params')
        paginated = config.get('paginated', False)

        if paginated:
            return self.api_connector.paginated_fetch(url)
        else:
            return self.api_connector.fetch_from_api(url, method, headers, params)

    def _ingest_custom_scrape(self, config):
        """Ingest using custom CSS selectors"""
        url = config.get('url')
        selectors = config.get('selectors')
        return self.scraper.scrape_custom(url, selectors)


def extract_data_from_file(file_storage):
    """Extract data from uploaded file (JSON or CSV)"""
    filename = file_storage.filename

    if filename.endswith(".json"):
        data = json.load(file_storage)
    elif filename.endswith(".csv"):
        file_storage.stream.seek(0)
        reader = csv.DictReader(file_storage.stream.read().decode("utf-8").splitlines())
        data = list(reader)
    else:
        data = []

    return data


# Global instance
multi_source_ingestion = MultiSourceIngestion()


# Example usage configurations
EXAMPLE_SOURCES = {
    'json_api': {
        'type': 'url_json',
        'url': 'https://api.example.com/data',
        'json_path': 'results'
    },
    'html_table': {
        'type': 'url_table',
        'url': 'https://example.com/table.html',
        'table_selector': 'table.data-table'
    },
    'custom_scrape': {
        'type': 'custom_scrape',
        'url': 'https://example.com/products',
        'selectors': {
            '_container': 'div.product',
            'name': 'h2.product-name',
            'price': 'span.price',
            'description': 'p.description'
        }
    }
}
