# load_categorized.py - Multi-Database Load with Categorization & Versioning

import logging
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from config_categorized import DatabaseConfig
import hashlib
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CategorizedDataLoader:
    """
    Advanced data loader with multi-database categorization,
    automatic versioning, and retention policies.
    """

    def __init__(self, config: DatabaseConfig = None):
        """Initialize loader with configuration"""
        self.config = config or DatabaseConfig()
        self.client = MongoClient(
            self.config.MONGO_URI,
            maxPoolSize=self.config.MAX_CONNECTIONS_PER_DB,
            serverSelectionTimeoutMS=self.config.CONNECTION_TIMEOUT_MS
        )
        logger.info(f"Connected to MongoDB: {self.config.MONGO_URI}")

    def auto_detect_category(self, record: Dict) -> str:
        """
        Automatically detect data category based on field names.

        Args:
            record: Sample record to analyze

        Returns:
            Detected category (e.g., 'ecommerce', 'hr', etc.)
        """
        if not record:
            return "uncategorized"

        fields = list(record.keys())
        detected = self.config.detect_source_from_fields(fields)

        logger.info(f"Auto-detected category: {detected} from fields: {fields[:5]}...")
        return detected

    def auto_detect_entity(self, record: Dict) -> str:
        """
        Automatically detect entity type from field names.

        Args:
            record: Sample record to analyze

        Returns:
            Detected entity type (e.g., 'products', 'employees', etc.)
        """
        if not record:
            return "data"

        fields = list(record.keys())
        detected = self.config.detect_entity_from_fields(fields)

        logger.info(f"Auto-detected entity: {detected} from fields: {fields[:5]}...")
        return detected

    def get_database(self, source: str):
        """Get database instance for a source"""
        db_name = self.config.get_db_for_source(source)
        return self.client[db_name]

    def compute_schema_hash(self, schema: Dict) -> str:
        """
        Compute hash of schema for quick comparison.

        Args:
            schema: Schema dictionary

        Returns:
            SHA256 hash of schema
        """
        schema_str = json.dumps(schema, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()

    def compute_schema_diff(self, old_schema: Dict, new_schema: Dict) -> Dict:
        """
        Compute detailed differences between schemas.

        Args:
            old_schema: Previous schema
            new_schema: Current schema

        Returns:
            Dictionary with added, removed, and modified fields
        """
        old_fields = set(old_schema.keys())
        new_fields = set(new_schema.keys())

        added = list(new_fields - old_fields)
        removed = list(old_fields - new_fields)

        # Check for type changes in common fields
        modified = []
        for field in old_fields & new_fields:
            old_type = old_schema[field].get("type", "unknown")
            new_type = new_schema[field].get("type", "unknown")

            if old_type != new_type:
                modified.append({
                    "field": field,
                    "old_type": old_type,
                    "new_type": new_type,
                    "old_semantic": old_schema[field].get("semantic_category", "unknown"),
                    "new_semantic": new_schema[field].get("semantic_category", "unknown"),
                })

        return {
            "added_fields": added,
            "removed_fields": removed,
            "modified_fields": modified,
            "breaking_changes": len(removed) > 0 or len(modified) > 0,
            "is_backward_compatible": len(removed) == 0,
        }

    def get_or_create_collection(
        self,
        source: str,
        entity: str,
        schema: Dict
    ) -> Tuple[Any, int, bool]:
        """
        Get collection with automatic versioning based on schema changes.

        Args:
            source: Data source category
            entity: Entity type
            schema: Current schema

        Returns:
            Tuple of (collection, version, is_new_version)
        """
        db = self.get_database(source)
        schema_collection = db[self.config.METADATA_COLLECTIONS["schema_versions"]]

        # Compute schema hash for quick comparison
        schema_hash = self.compute_schema_hash(schema)

        # Find latest schema version for this source/entity
        latest_schema = schema_collection.find_one(
            {"source": source, "entity": entity},
            sort=[("version", DESCENDING)]
        )

        if latest_schema:
            # Check if schema changed
            latest_hash = latest_schema.get("schema_hash", "")

            if latest_hash != schema_hash:
                # Schema evolved - create new version
                new_version = latest_schema["version"] + 1
                collection_name = self.config.get_collection_name(source, entity, new_version)

                # Compute differences
                diff = self.compute_schema_diff(latest_schema["schema"], schema)

                # Save new schema version
                schema_doc = {
                    "source": source,
                    "entity": entity,
                    "version": new_version,
                    "schema": schema,
                    "schema_hash": schema_hash,
                    "created_at": datetime.now(),
                    "parent_version": latest_schema["version"],
                    "changes": diff,
                    "record_count": 0,
                }

                schema_collection.insert_one(schema_doc)

                logger.info(
                    f"📊 Schema evolved: {source}.{entity} "
                    f"v{latest_schema['version']} → v{new_version}"
                )
                logger.info(f"   Added: {diff['added_fields']}")
                logger.info(f"   Removed: {diff['removed_fields']}")
                logger.info(f"   Modified: {len(diff['modified_fields'])} fields")

                is_new = True
            else:
                # Same schema - use existing collection
                new_version = latest_schema["version"]
                collection_name = self.config.get_collection_name(source, entity, new_version)

                # Update last_used timestamp
                schema_collection.update_one(
                    {"_id": latest_schema["_id"]},
                    {"$set": {"last_used": datetime.now()}}
                )

                logger.info(f"✅ Using existing schema: {source}.{entity} v{new_version}")
                is_new = False
        else:
            # First version
            new_version = 1
            collection_name = self.config.get_collection_name(source, entity, new_version)

            schema_doc = {
                "source": source,
                "entity": entity,
                "version": new_version,
                "schema": schema,
                "schema_hash": schema_hash,
                "created_at": datetime.now(),
                "parent_version": None,
                "changes": None,
                "record_count": 0,
            }

            schema_collection.insert_one(schema_doc)

            logger.info(f"🆕 Created new schema: {source}.{entity} v{new_version}")
            is_new = True

        # Create indexes on the collection
        collection = db[collection_name]
        self._create_indexes(collection, schema, source, entity)

        return collection, new_version, is_new

    def _create_indexes(self, collection, schema: Dict, source: str, entity: str):
        """Create indexes on collection based on schema"""
        try:
            # Create index on metadata fields
            collection.create_index([("_metadata.loaded_at", DESCENDING)])
            collection.create_index([("_metadata.source", ASCENDING)])

            # Create indexes on identifier fields
            identifier_fields = ['id', 'email', 'user', 'name', 'customer_id', 'order_id']
            for field in identifier_fields:
                if field in schema:
                    collection.create_index([(field, ASCENDING)])
                    logger.debug(f"Created index on {field}")

            # Create indexes on frequently queried fields
            if source == "ecommerce":
                for field in ['price', 'category', 'sku']:
                    if field in schema:
                        collection.create_index([(field, ASCENDING)])
            elif source == "hr":
                for field in ['department', 'hire_date']:
                    if field in schema:
                        collection.create_index([(field, ASCENDING)])

        except Exception as e:
            logger.warning(f"Failed to create some indexes: {e}")

    def deduplicate_batch(
        self,
        collection,
        batch: List[Dict],
        identifier_fields: List[str] = None
    ) -> Tuple[List[Dict], int]:
        """
        Remove duplicates from batch and check against existing data.

        Args:
            collection: MongoDB collection
            batch: List of records
            identifier_fields: Fields to use for deduplication

        Returns:
            Tuple of (unique_records, duplicate_count)
        """
        if identifier_fields is None:
            identifier_fields = ['id', 'name', 'email', 'user', 'customer_id', 'order_id']

        unique_records = []
        seen_identifiers = set()
        dup_count = 0

        for record in batch:
            # Create identifier for this record
            identifier = None
            for field in identifier_fields:
                if field in record and record[field]:
                    identifier = (field, str(record[field]))
                    break

            if identifier:
                # Check if we've seen this in current batch
                if identifier not in seen_identifiers:
                    # Check if exists in database
                    query = {identifier[0]: record[identifier[0]]}
                    existing = collection.find_one(query, {"_id": 1})

                    if not existing:
                        unique_records.append(record)
                        seen_identifiers.add(identifier)
                    else:
                        dup_count += 1
                else:
                    dup_count += 1
            else:
                # No identifier found, include record
                unique_records.append(record)

        if dup_count > 0:
            logger.info(f"🗑️  Removed {dup_count} duplicates from batch")

        return unique_records, dup_count

    def detect_changes(
        self,
        collection,
        batch: List[Dict],
        source: str,
        entity: str
    ) -> List[Dict]:
        """
        Detect changes in key fields compared to existing data.

        Args:
            collection: MongoDB collection
            batch: New batch of records
            source: Data source
            entity: Entity type

        Returns:
            List of detected changes
        """
        changes_detected = []

        # Define key fields to monitor per source
        key_fields_map = {
            "ecommerce": ['price', 'discount', 'stock', 'rating'],
            "hr": ['salary', 'department', 'job_title'],
            "iot_sensors": ['reading', 'status'],
            "financial": ['balance', 'amount'],
        }

        key_fields = key_fields_map.get(source, ['price', 'discount', 'score', 'rating', 'salary'])

        identifier_fields = ['id', 'name', 'user', 'email', 'customer_id']

        for record in batch:
            # Find identifier
            query = {}
            for field in identifier_fields:
                if field in record and record[field]:
                    query[field] = record[field]
                    break

            if query:
                existing_record = collection.find_one(query)

                if existing_record:
                    # Check for changes in key fields
                    for key_field in key_fields:
                        if key_field in record and key_field in existing_record:
                            old_value = existing_record.get(key_field)
                            new_value = record.get(key_field)

                            if old_value != new_value:
                                change = {
                                    "source": source,
                                    "entity": entity,
                                    "identifier": query,
                                    "field": key_field,
                                    "old_value": old_value,
                                    "new_value": new_value,
                                    "timestamp": datetime.now(),
                                    "change_type": "update",
                                    "change_magnitude": self._calculate_change_magnitude(
                                        old_value, new_value
                                    ),
                                }
                                changes_detected.append(change)

        if changes_detected:
            # Log changes to metadata collection
            db = self.get_database(source)
            changes_collection = db[self.config.METADATA_COLLECTIONS["data_changes"]]
            changes_collection.insert_many(changes_detected)

            logger.info(f"📈 Detected {len(changes_detected)} changes in {source}.{entity}")

        return changes_detected

    def _calculate_change_magnitude(self, old_value, new_value) -> Optional[float]:
        """Calculate magnitude of change for numeric values"""
        try:
            old_num = float(old_value)
            new_num = float(new_value)
            if old_num != 0:
                return ((new_num - old_num) / old_num) * 100  # Percentage change
            else:
                return None
        except (ValueError, TypeError):
            return None

    def apply_retention_policy(self, source: str):
        """
        Apply retention policy for a source - delete old data.

        Args:
            source: Data source category
        """
        if not self.config.ENABLE_RETENTION_POLICIES:
            return

        retention_days = self.config.get_retention_days(source)
        cutoff_date = datetime.now() - timedelta(days=retention_days)

        db = self.get_database(source)

        # Find all collections for this source
        deleted_total = 0
        for collection_name in db.list_collection_names():
            if collection_name.startswith(source) and not collection_name.startswith("_"):
                result = db[collection_name].delete_many({
                    "_metadata.loaded_at": {"$lt": cutoff_date}
                })
                deleted_total += result.deleted_count

        if deleted_total > 0:
            logger.info(
                f"🗑️  Applied retention policy to {source}: "
                f"Deleted {deleted_total} records older than {retention_days} days"
            )

    def load_categorized_data(
        self,
        batch: List[Dict],
        source: str,
        entity: str,
        schema: Dict,
        deduplicate: bool = True,
        detect_change: bool = True
    ) -> Dict[str, Any]:
        """
        Load data with categorization and versioning.

        Args:
            batch: List of records to load
            source: Data source category
            entity: Entity type
            schema: Schema dictionary
            deduplicate: Whether to remove duplicates
            detect_change: Whether to detect changes

        Returns:
            Dictionary with loading statistics
        """
        # Get appropriate collection with versioning
        collection, version, is_new_version = self.get_or_create_collection(
            source, entity, schema
        )

        # Deduplication
        dup_count = 0
        if deduplicate:
            batch, dup_count = self.deduplicate_batch(collection, batch)

        # Change detection
        changes = []
        if detect_change and not is_new_version:
            changes = self.detect_changes(collection, batch, source, entity)

        # Add metadata to each record
        for record in batch:
            record['_metadata'] = {
                'source': source,
                'entity': entity,
                'version': version,
                'loaded_at': datetime.now(),
                'database': self.config.get_db_for_source(source),
                'collection': collection.name,
            }

        # Bulk insert
        inserted_count = 0
        if batch:
            try:
                result = collection.insert_many(batch, ordered=False)
                inserted_count = len(result.inserted_ids)

                # Update record count in schema
                db = self.get_database(source)
                schema_collection = db[self.config.METADATA_COLLECTIONS["schema_versions"]]
                schema_collection.update_one(
                    {"source": source, "entity": entity, "version": version},
                    {"$inc": {"record_count": inserted_count}}
                )

                logger.info(
                    f"✅ Loaded {inserted_count} records to "
                    f"{source}.{entity}_v{version}"
                )
            except BulkWriteError as bwe:
                inserted_count = bwe.details.get('nInserted', 0)
                logger.error(f"Bulk write error: {bwe.details}")
            except Exception as e:
                logger.error(f"Insert failed: {e}")

        # Apply retention policy
        self.apply_retention_policy(source)

        return {
            'inserted_count': inserted_count,
            'duplicate_count': dup_count,
            'change_count': len(changes),
            'changes': changes,
            'version': version,
            'is_new_version': is_new_version,
            'source': source,
            'entity': entity,
            'database': self.config.get_db_for_source(source),
            'collection': collection.name,
        }

    def query_across_versions(
        self,
        source: str,
        entity: str,
        query: Dict = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        Query data across all versions of a source/entity.

        Args:
            source: Data source
            entity: Entity type
            query: MongoDB query filter
            limit: Maximum results

        Returns:
            List of matching records from all versions
        """
        db = self.get_database(source)
        schema_collection = db[self.config.METADATA_COLLECTIONS["schema_versions"]]

        # Find all versions
        versions = schema_collection.find(
            {"source": source, "entity": entity},
            sort=[("version", ASCENDING)]
        )

        results = []
        for version_doc in versions:
            version = version_doc["version"]
            collection_name = self.config.get_collection_name(source, entity, version)

            if collection_name in db.list_collection_names():
                data = list(db[collection_name].find(query or {}).limit(limit))
                results.extend(data)

        logger.info(
            f"📊 Query across versions: Found {len(results)} records "
            f"for {source}.{entity}"
        )
        return results

    def get_schema_history(self, source: str, entity: str) -> List[Dict]:
        """
        Get schema evolution history for a source/entity.

        Args:
            source: Data source
            entity: Entity type

        Returns:
            List of schema versions with metadata
        """
        db = self.get_database(source)
        schema_collection = db[self.config.METADATA_COLLECTIONS["schema_versions"]]

        history = list(schema_collection.find(
            {"source": source, "entity": entity},
            sort=[("version", ASCENDING)]
        ))

        return history

    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get statistics across all categorized databases.

        Returns:
            Dictionary with database statistics
        """
        stats = {
            'databases': {},
            'total_records': 0,
            'total_collections': 0,
        }

        for source, db_name in self.config.SOURCE_DB_MAPPING.items():
            db = self.client[db_name]
            collections = [c for c in db.list_collection_names()
                          if not c.startswith('system.')]

            db_stats = {
                'collections': len(collections),
                'records': 0,
            }

            for coll_name in collections:
                count = db[coll_name].count_documents({})
                db_stats['records'] += count

            stats['databases'][source] = db_stats
            stats['total_records'] += db_stats['records']
            stats['total_collections'] += db_stats['collections']

        return stats

    def close(self):
        """Close MongoDB connection"""
        self.client.close()
        logger.info("MongoDB connection closed")
