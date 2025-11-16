import logging
from pymongo import MongoClient, errors
from datetime import datetime
from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION, MONGO_SCHEMA_COLLECTION, MONGO_CHANGES_COLLECTION

def get_db():
    """Get MongoDB database connection."""
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]

def get_collection():
    """Get main data collection."""
    db = get_db()
    return db[MONGO_COLLECTION]

def get_schema_collection():
    """Get schema versions collection."""
    db = get_db()
    return db[MONGO_SCHEMA_COLLECTION]

def get_changes_collection():
    """Get data changes tracking collection."""
    db = get_db()
    return db[MONGO_CHANGES_COLLECTION]

def save_schema_version(schema, stats):
    """Save schema version with timestamp."""
    schema_collection = get_schema_collection()

    # Check if this exact schema already exists
    existing = schema_collection.find_one({"schema": schema})

    if not existing:
        schema_doc = {
            "schema": schema,
            "version": schema_collection.count_documents({}) + 1,
            "created_at": datetime.now(),
            "stats": stats
        }
        schema_collection.insert_one(schema_doc)
        logging.info(f"New schema version {schema_doc['version']} saved.")
        return schema_doc['version']
    else:
        # Update last_used timestamp
        schema_collection.update_one(
            {"_id": existing["_id"]},
            {"$set": {"last_used": datetime.now()}}
        )
        logging.info(f"Using existing schema version {existing['version']}.")
        return existing['version']

def detect_changes(new_batch):
    """Detect changes in key fields compared to existing data."""
    changes_detected = []
    collection = get_collection()

    # Define key fields to monitor (can be made configurable)
    key_fields = ['price', 'discount', 'score', 'rating', 'salary']

    for record in new_batch:
        # Try to find existing record (assuming there's some unique identifier)
        # For demo, we'll check by name or similar fields
        identifier_fields = ['name', 'user', 'email', 'id']
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
                                "identifier": query,
                                "field": key_field,
                                "old_value": old_value,
                                "new_value": new_value,
                                "timestamp": datetime.now(),
                                "change_type": "update"
                            }
                            changes_detected.append(change)

    if changes_detected:
        changes_collection = get_changes_collection()
        changes_collection.insert_many(changes_detected)
        logging.info(f"Detected {len(changes_detected)} changes in data.")

    return changes_detected

def deduplicate_batch(batch):
    """Remove duplicates from batch and check against existing data."""
    collection = get_collection()

    # Define identifier fields for deduplication
    identifier_fields = ['name', 'user', 'email', 'id']

    unique_records = []
    seen_identifiers = set()
    duplicates_in_batch = 0

    for record in batch:
        # Create identifier for this record
        identifier = None
        for field in identifier_fields:
            if field in record and record[field]:
                identifier = (field, record[field])
                break

        if identifier:
            # Check if we've seen this in current batch
            if identifier not in seen_identifiers:
                # Check if exists in database
                query = {identifier[0]: identifier[1]}
                existing = collection.find_one(query)

                if not existing:
                    unique_records.append(record)
                    seen_identifiers.add(identifier)
                else:
                    duplicates_in_batch += 1
                    logging.info(f"Duplicate found: {identifier}")
            else:
                duplicates_in_batch += 1
        else:
            # No identifier found, include record
            unique_records.append(record)

    if duplicates_in_batch > 0:
        logging.info(f"Removed {duplicates_in_batch} duplicates from batch.")

    return unique_records, duplicates_in_batch

def load_data(batch, detect_change=True, deduplicate=True):
    """Load data into MongoDB with optional change detection and deduplication."""
    collection = get_collection()

    # Deduplicate batch
    duplicates_count = 0
    if deduplicate:
        batch, duplicates_count = deduplicate_batch(batch)

    # Detect changes before inserting
    changes = []
    if detect_change:
        changes = detect_changes(batch)

    try:
        if batch:
            # Add metadata to each record
            for record in batch:
                record['_loaded_at'] = datetime.now()

            collection.insert_many(batch, ordered=False)
            logging.info(f"Inserted {len(batch)} records.")
            return changes, duplicates_count
        else:
            logging.info("No new records to insert after deduplication.")
            return changes, duplicates_count
    except errors.BulkWriteError as bwe:
        logging.error("Bulk write error: %s", bwe.details)
        return changes, duplicates_count
    except Exception as ex:
        logging.error("Insert failed: %s", ex)
        return changes, duplicates_count
