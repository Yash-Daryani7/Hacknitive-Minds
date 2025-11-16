# api_categorized.py - REST API for Categorized Data Operations

from flask import Blueprint, request, jsonify
from load_categorized import CategorizedDataLoader
from config_categorized import DatabaseConfig
from bson import ObjectId
from datetime import datetime
import logging

api_categorized = Blueprint('api_categorized', __name__, url_prefix='/api/v2')

# Initialize loader
loader = CategorizedDataLoader(DatabaseConfig())

logger = logging.getLogger(__name__)


def sanitize_for_json(data):
    """Convert MongoDB objects to JSON-serializable format"""
    if isinstance(data, list):
        return [sanitize_for_json(item) for item in data]
    elif isinstance(data, dict):
        sanitized = {}
        for key, value in data.items():
            if key == '_id':
                continue
            elif isinstance(value, ObjectId):
                sanitized[key] = str(value)
            elif isinstance(value, datetime):
                sanitized[key] = value.isoformat()
            elif isinstance(value, dict):
                sanitized[key] = sanitize_for_json(value)
            elif isinstance(value, list):
                sanitized[key] = sanitize_for_json(value)
            else:
                sanitized[key] = value
        return sanitized
    elif isinstance(data, datetime):
        return data.isoformat()
    elif isinstance(data, ObjectId):
        return str(data)
    else:
        return data


@api_categorized.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "categorized-etl-pipeline",
        "version": "2.0.0",
        "features": [
            "multi-database-categorization",
            "automatic-versioning",
            "retention-policies",
            "auto-detection"
        ]
    })


@api_categorized.route('/sources', methods=['GET'])
def list_sources():
    """List all available data source categories"""
    return jsonify({
        "status": "success",
        "sources": list(DatabaseConfig.SOURCE_DB_MAPPING.keys()),
        "count": len(DatabaseConfig.SOURCE_DB_MAPPING)
    })


@api_categorized.route('/databases/stats', methods=['GET'])
def database_stats():
    """Get statistics across all categorized databases"""
    try:
        stats = loader.get_database_stats()
        return jsonify({
            "status": "success",
            "data": sanitize_for_json(stats)
        })
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@api_categorized.route('/schema/history', methods=['GET'])
def schema_history():
    """
    Get schema evolution history for a source/entity

    Query params:
        - source: Data source category
        - entity: Entity type
    """
    source = request.args.get('source')
    entity = request.args.get('entity')

    if not source or not entity:
        return jsonify({
            "status": "error",
            "message": "Both 'source' and 'entity' parameters are required"
        }), 400

    try:
        history = loader.get_schema_history(source, entity)
        return jsonify({
            "status": "success",
            "source": source,
            "entity": entity,
            "version_count": len(history),
            "history": sanitize_for_json(history)
        })
    except Exception as e:
        logger.error(f"Error getting schema history: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@api_categorized.route('/query', methods=['POST'])
def query_data():
    """
    Query data from a specific source/entity/version

    JSON body:
        - source: Data source category
        - entity: Entity type
        - version: (optional) Specific version, defaults to latest
        - query: (optional) MongoDB query filter
        - limit: (optional) Maximum results, default 100
    """
    data = request.get_json()

    source = data.get('source')
    entity = data.get('entity')
    version = data.get('version')  # Optional
    query_filter = data.get('query', {})
    limit = data.get('limit', 100)

    if not source or not entity:
        return jsonify({
            "status": "error",
            "message": "Both 'source' and 'entity' are required"
        }), 400

    try:
        # Get database and collection
        db = loader.get_database(source)

        if version:
            # Query specific version
            collection_name = DatabaseConfig.get_collection_name(source, entity, version)
        else:
            # Get latest version
            schema_collection = db[DatabaseConfig.METADATA_COLLECTIONS["schema_versions"]]
            latest = schema_collection.find_one(
                {"source": source, "entity": entity},
                sort=[("version", -1)]
            )

            if not latest:
                return jsonify({
                    "status": "error",
                    "message": f"No data found for {source}.{entity}"
                }), 404

            version = latest["version"]
            collection_name = DatabaseConfig.get_collection_name(source, entity, version)

        # Execute query
        results = list(db[collection_name].find(query_filter).limit(limit))

        return jsonify({
            "status": "success",
            "source": source,
            "entity": entity,
            "version": version,
            "count": len(results),
            "data": sanitize_for_json(results)
        })

    except Exception as e:
        logger.error(f"Error querying data: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@api_categorized.route('/query/across_versions', methods=['POST'])
def query_across_versions():
    """
    Query data across all versions of a source/entity

    JSON body:
        - source: Data source category
        - entity: Entity type
        - query: (optional) MongoDB query filter
        - limit: (optional) Maximum results per version, default 100
    """
    data = request.get_json()

    source = data.get('source')
    entity = data.get('entity')
    query_filter = data.get('query', {})
    limit = data.get('limit', 100)

    if not source or not entity:
        return jsonify({
            "status": "error",
            "message": "Both 'source' and 'entity' are required"
        }), 400

    try:
        results = loader.query_across_versions(source, entity, query_filter, limit)

        return jsonify({
            "status": "success",
            "source": source,
            "entity": entity,
            "total_count": len(results),
            "data": sanitize_for_json(results)
        })

    except Exception as e:
        logger.error(f"Error querying across versions: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@api_categorized.route('/changes', methods=['GET'])
def get_changes():
    """
    Get change history for a source/entity

    Query params:
        - source: Data source category
        - entity: Entity type
        - limit: (optional) Maximum results, default 50
    """
    source = request.args.get('source')
    entity = request.args.get('entity')
    limit = int(request.args.get('limit', 50))

    if not source or not entity:
        return jsonify({
            "status": "error",
            "message": "Both 'source' and 'entity' parameters are required"
        }), 400

    try:
        db = loader.get_database(source)
        changes_collection = db[DatabaseConfig.METADATA_COLLECTIONS["data_changes"]]

        changes = list(changes_collection.find(
            {"source": source, "entity": entity}
        ).sort("timestamp", -1).limit(limit))

        return jsonify({
            "status": "success",
            "source": source,
            "entity": entity,
            "count": len(changes),
            "changes": sanitize_for_json(changes)
        })

    except Exception as e:
        logger.error(f"Error getting changes: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@api_categorized.route('/auto_categorize', methods=['POST'])
def auto_categorize():
    """
    Auto-detect source and entity from sample record

    JSON body:
        - sample_record: Dictionary with sample data fields
    """
    data = request.get_json()
    sample_record = data.get('sample_record')

    if not sample_record or not isinstance(sample_record, dict):
        return jsonify({
            "status": "error",
            "message": "sample_record (dict) is required"
        }), 400

    try:
        detected_source = loader.auto_detect_category(sample_record)
        detected_entity = loader.auto_detect_entity(sample_record)

        return jsonify({
            "status": "success",
            "detected": {
                "source": detected_source,
                "entity": detected_entity,
                "database": DatabaseConfig.get_db_for_source(detected_source)
            },
            "sample_fields": list(sample_record.keys())
        })

    except Exception as e:
        logger.error(f"Error auto-categorizing: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@api_categorized.route('/retention_policy', methods=['GET'])
def get_retention_policy():
    """
    Get retention policy for a source

    Query params:
        - source: Data source category
    """
    source = request.args.get('source')

    if not source:
        return jsonify({
            "status": "error",
            "message": "source parameter is required"
        }), 400

    try:
        retention_days = DatabaseConfig.get_retention_days(source)

        return jsonify({
            "status": "success",
            "source": source,
            "retention_days": retention_days,
            "retention_years": round(retention_days / 365, 2)
        })

    except Exception as e:
        logger.error(f"Error getting retention policy: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@api_categorized.route('/collection/info', methods=['GET'])
def collection_info():
    """
    Get detailed information about a specific collection

    Query params:
        - source: Data source category
        - entity: Entity type
        - version: Schema version
    """
    source = request.args.get('source')
    entity = request.args.get('entity')
    version = request.args.get('version')

    if not all([source, entity, version]):
        return jsonify({
            "status": "error",
            "message": "source, entity, and version parameters are required"
        }), 400

    try:
        version = int(version)
        db = loader.get_database(source)
        collection_name = DatabaseConfig.get_collection_name(source, entity, version)

        if collection_name not in db.list_collection_names():
            return jsonify({
                "status": "error",
                "message": f"Collection {collection_name} not found"
            }), 404

        collection = db[collection_name]

        # Get collection stats
        record_count = collection.count_documents({})
        sample_record = collection.find_one()
        indexes = list(collection.list_indexes())

        # Get schema info
        schema_collection = db[DatabaseConfig.METADATA_COLLECTIONS["schema_versions"]]
        schema_doc = schema_collection.find_one({
            "source": source,
            "entity": entity,
            "version": version
        })

        return jsonify({
            "status": "success",
            "collection": collection_name,
            "database": db.name,
            "record_count": record_count,
            "indexes": [idx['name'] for idx in indexes],
            "schema": sanitize_for_json(schema_doc) if schema_doc else None,
            "sample_record": sanitize_for_json(sample_record) if sample_record else None
        })

    except Exception as e:
        logger.error(f"Error getting collection info: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# Error handlers
@api_categorized.errorhandler(404)
def not_found(error):
    return jsonify({
        "status": "error",
        "message": "Endpoint not found"
    }), 404


@api_categorized.errorhandler(500)
def internal_error(error):
    return jsonify({
        "status": "error",
        "message": "Internal server error"
    }), 500
