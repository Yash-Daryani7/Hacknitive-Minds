# config_categorized.py - Multi-Database Categorization Configuration

import os
from typing import Dict, List
from datetime import timedelta

class DatabaseConfig:
    """Configuration for multi-database categorization with versioning"""

    # MongoDB Connection
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

    # Default database for uncategorized data
    DEFAULT_DB = "pipeline_default"

    # Source-to-Database mapping
    # Each data source gets its own database for isolation
    SOURCE_DB_MAPPING = {
        "ecommerce": "ecommerce_db",
        "hr": "hr_db",
        "api_logs": "api_logs_db",
        "iot_sensors": "iot_sensors_db",
        "web_scraping": "web_scraping_db",
        "social_media": "social_media_db",
        "financial": "financial_db",
        "healthcare": "healthcare_db",
        "marketing": "marketing_db",
        "customer_data": "customer_data_db",
        "inventory": "inventory_db",
        "sales": "sales_db",
    }

    # Retention policies per source (in days)
    # Determines how long data is kept before auto-deletion
    RETENTION_POLICIES = {
        "ecommerce": 2555,      # 7 years (tax compliance)
        "hr": 3650,             # 10 years (employment records)
        "api_logs": 30,         # 30 days (operational logs)
        "iot_sensors": 90,      # 90 days (sensor data)
        "web_scraping": 180,    # 6 months (scraped data)
        "social_media": 365,    # 1 year (social posts)
        "financial": 3650,      # 10 years (financial compliance)
        "healthcare": 7300,     # 20 years (medical records)
        "marketing": 730,       # 2 years (campaign data)
        "customer_data": 1825,  # 5 years (GDPR compliance)
        "inventory": 1095,      # 3 years (inventory history)
        "sales": 2555,          # 7 years (sales records)
    }

    # Collection naming pattern: {source}_{entity}_v{version}
    # Example: ecommerce_products_v1, hr_employees_v2
    COLLECTION_PATTERN = "{source}_{entity}_v{version}"

    # Metadata collections (present in each database)
    METADATA_COLLECTIONS = {
        "schema_versions": "schema_versions",
        "data_changes": "data_changes",
        "data_lineage": "data_lineage",
        "access_logs": "access_logs",
        "quality_metrics": "quality_metrics",
    }

    # Auto-categorization rules based on field names
    # Used to automatically detect data source when not specified
    AUTO_CATEGORIZATION_RULES = {
        "ecommerce": ["price", "product", "sku", "cart", "order", "checkout", "inventory", "shipping"],
        "hr": ["employee", "salary", "department", "hire_date", "job_title", "manager", "payroll"],
        "api_logs": ["endpoint", "status_code", "response_time", "method", "api_key", "request_id"],
        "iot_sensors": ["temperature", "humidity", "sensor_id", "device_id", "reading", "timestamp"],
        "web_scraping": ["url", "scraped_at", "html_content", "page_title", "meta_description"],
        "social_media": ["post", "likes", "shares", "comments", "followers", "hashtag", "mention"],
        "financial": ["transaction", "amount", "account", "balance", "payment", "invoice", "ledger"],
        "healthcare": ["patient", "diagnosis", "treatment", "medication", "doctor", "appointment"],
        "marketing": ["campaign", "impression", "click", "conversion", "ad_spend", "roi"],
        "customer_data": ["customer_id", "email", "phone", "address", "loyalty_points"],
        "inventory": ["stock", "warehouse", "bin_location", "quantity_on_hand"],
        "sales": ["invoice", "revenue", "discount", "tax", "total_amount"],
    }

    # Entity type detection (what kind of data is this?)
    ENTITY_DETECTION_RULES = {
        "products": ["product", "sku", "price", "category"],
        "orders": ["order", "order_id", "total", "customer"],
        "employees": ["employee", "staff", "worker", "hire_date"],
        "customers": ["customer", "client", "buyer", "user"],
        "transactions": ["transaction", "payment", "amount"],
        "logs": ["log", "timestamp", "level", "message"],
        "sensors": ["sensor", "reading", "measurement"],
        "events": ["event", "occurred_at", "event_type"],
    }

    # Batch size for processing
    BATCH_SIZE = 1000

    # Enable/disable features
    ENABLE_AUTO_CATEGORIZATION = True
    ENABLE_RETENTION_POLICIES = True
    ENABLE_DATA_LINEAGE = True
    ENABLE_ACCESS_LOGGING = True
    ENABLE_QUALITY_METRICS = True

    # Performance settings
    MAX_CONNECTIONS_PER_DB = 100
    CONNECTION_TIMEOUT_MS = 5000

    # Data quality thresholds
    QUALITY_SCORE_THRESHOLDS = {
        "excellent": 95,
        "good": 80,
        "fair": 60,
        "poor": 40,
    }

    @classmethod
    def get_db_for_source(cls, source: str) -> str:
        """Get database name for a data source"""
        return cls.SOURCE_DB_MAPPING.get(source.lower(), cls.DEFAULT_DB)

    @classmethod
    def get_retention_days(cls, source: str) -> int:
        """Get retention policy for a source (in days)"""
        return cls.RETENTION_POLICIES.get(source.lower(), 365)  # Default 1 year

    @classmethod
    def get_collection_name(cls, source: str, entity: str, version: int) -> str:
        """Generate collection name with versioning"""
        return cls.COLLECTION_PATTERN.format(
            source=source.lower(),
            entity=entity.lower(),
            version=version
        )

    @classmethod
    def detect_source_from_fields(cls, fields: List[str]) -> str:
        """Auto-detect source category from field names"""
        if not cls.ENABLE_AUTO_CATEGORIZATION:
            return "uncategorized"

        field_set = {f.lower() for f in fields}

        # Count matches for each category
        scores = {}
        for category, keywords in cls.AUTO_CATEGORIZATION_RULES.items():
            score = sum(1 for keyword in keywords
                       if any(keyword in field for field in field_set))
            if score > 0:
                scores[category] = score

        # Return category with highest score
        if scores:
            return max(scores.items(), key=lambda x: x[1])[0]

        return "uncategorized"

    @classmethod
    def detect_entity_from_fields(cls, fields: List[str]) -> str:
        """Auto-detect entity type from field names"""
        field_set = {f.lower() for f in fields}

        # Count matches for each entity type
        scores = {}
        for entity, keywords in cls.ENTITY_DETECTION_RULES.items():
            score = sum(1 for keyword in keywords
                       if any(keyword in field for field in field_set))
            if score > 0:
                scores[entity] = score

        # Return entity with highest score
        if scores:
            return max(scores.items(), key=lambda x: x[1])[0]

        return "data"  # Default entity name


# Backward compatibility with original config
MONGO_URI = DatabaseConfig.MONGO_URI
MONGO_DB = DatabaseConfig.DEFAULT_DB
MONGO_COLLECTION = "entries"
MONGO_SCHEMA_COLLECTION = "schema_versions"
MONGO_CHANGES_COLLECTION = "data_changes"
BATCH_SIZE = DatabaseConfig.BATCH_SIZE
