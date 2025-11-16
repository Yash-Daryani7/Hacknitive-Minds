"""
REST API Endpoints for Data Ingestion
Provides programmatic access to the pipeline
"""

from flask import Blueprint, request, jsonify
import logging
from datetime import datetime

from extract import extract_data, batch_data
from ai_schema_inference import infer_schema_with_ai, ai_schema_inferencer
from ml_data_processing import process_data_with_ml
from transform import transform_batch
from load import load_data, save_schema_version, get_collection, get_schema_collection, get_changes_collection
from data_sources import multi_source_ingestion
from analytics_engine import trend_analyzer, change_analyzer, recommendation_engine, data_insights
from config import BATCH_SIZE

# Create Blueprint
api = Blueprint('api', __name__, url_prefix='/api/v1')


@api.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    })


@api.route('/ingest', methods=['POST'])
def ingest_data():
    """
    Ingest data from various sources
    POST /api/v1/ingest

    Request body:
    {
        "source_type": "file|url_json|url_table|api|custom_scrape",
        "data": [...] or "url": "...",
        "config": {...}
    }
    """
    try:
        req_data = request.get_json()

        if not req_data:
            return jsonify({'error': 'No data provided'}), 400

        source_type = req_data.get('source_type', 'data')

        # Handle different ingestion types
        if source_type == 'data':
            # Direct data input
            data = req_data.get('data', [])
        else:
            # Use multi-source ingestion
            source_config = {
                'type': source_type,
                **req_data.get('config', {})
            }
            data = multi_source_ingestion.ingest_from_source(source_config)

        if not data:
            return jsonify({'error': 'No data to ingest'}), 400

        # Process data through AI pipeline
        stats = {
            'total_records': len(data),
            'total_fields': 0,
            'fields_by_type': {},
            'changes_detected': 0,
            'duplicates_removed': 0,
            'anomalies_detected': 0,
            'inserted_records': 0
        }

        schema = {}
        all_changes = []
        total_duplicates = 0
        total_anomalies = 0

        for batch in batch_data(data, BATCH_SIZE):
            # AI-powered schema inference
            schema = infer_schema_with_ai(batch, schema)

            # ML-powered data processing
            processed_batch, dup_count, anomaly_count = process_data_with_ml(batch, schema)

            total_duplicates += dup_count
            total_anomalies += anomaly_count

            # Transform and load
            t_batch = transform_batch(processed_batch, schema)
            changes, _ = load_data(t_batch)

            if changes:
                all_changes.extend(changes)

        # Update stats
        stats['total_fields'] = len(schema)
        stats['changes_detected'] = len(all_changes)
        stats['duplicates_removed'] = total_duplicates
        stats['anomalies_detected'] = total_anomalies
        stats['inserted_records'] = len(data) - total_duplicates

        # Count fields by type
        for field_name, field_info in schema.items():
            field_type = field_info.get('type', 'unknown')
            stats['fields_by_type'][field_type] = stats['fields_by_type'].get(field_type, 0) + 1

        # Save schema version
        schema_version = save_schema_version(schema, stats)

        # Generate recommendations
        recommendations = recommendation_engine.generate_recommendations(schema, stats)

        return jsonify({
            'status': 'success',
            'message': f'Processed {len(data)} records',
            'stats': stats,
            'schema_version': schema_version,
            'schema': schema,
            'recommendations': recommendations
        }), 200

    except Exception as e:
        logging.error(f"Ingestion failed: {e}")
        return jsonify({'error': str(e)}), 500


@api.route('/schema', methods=['GET'])
def get_schema():
    """
    Get current or specific schema version
    GET /api/v1/schema?version=<version_number>
    """
    try:
        version = request.args.get('version', type=int)
        schema_collection = get_schema_collection()

        if version:
            schema_doc = schema_collection.find_one({'version': version})
        else:
            schema_doc = schema_collection.find_one({}, sort=[('version', -1)])

        if not schema_doc:
            return jsonify({'error': 'Schema not found'}), 404

        # Convert ObjectId to string for JSON serialization
        schema_doc['_id'] = str(schema_doc['_id'])

        return jsonify(schema_doc), 200

    except Exception as e:
        logging.error(f"Get schema failed: {e}")
        return jsonify({'error': str(e)}), 500


@api.route('/schema/versions', methods=['GET'])
def get_schema_versions():
    """
    Get all schema versions
    GET /api/v1/schema/versions
    """
    try:
        schema_collection = get_schema_collection()
        schemas = list(schema_collection.find({}, {'version': 1, 'created_at': 1, 'stats': 1}).sort('version', -1))

        for schema in schemas:
            schema['_id'] = str(schema['_id'])

        return jsonify({'schemas': schemas, 'count': len(schemas)}), 200

    except Exception as e:
        logging.error(f"Get schema versions failed: {e}")
        return jsonify({'error': str(e)}), 500


@api.route('/analytics/trends', methods=['GET'])
def get_trends():
    """
    Get trend analysis for fields
    GET /api/v1/analytics/trends?field=<field_name>&days=<days>
    """
    try:
        field = request.args.get('field')
        days = request.args.get('days', default=30, type=int)

        if field:
            # Get trend for specific field
            trend = trend_analyzer.analyze_field_trends(field, days)
            return jsonify({'field': field, 'trend': trend}), 200
        else:
            # Get trends for all numeric fields
            schema_collection = get_schema_collection()
            latest_schema = schema_collection.find_one({}, sort=[('version', -1)])

            if not latest_schema:
                return jsonify({'error': 'No schema found'}), 404

            schema = latest_schema.get('schema', {})
            trends = trend_analyzer.get_all_trends(schema)

            return jsonify({'trends': trends}), 200

    except Exception as e:
        logging.error(f"Get trends failed: {e}")
        return jsonify({'error': str(e)}), 500


@api.route('/analytics/changes', methods=['GET'])
def get_changes():
    """
    Get change detection history
    GET /api/v1/analytics/changes?days=<days>&limit=<limit>
    """
    try:
        days = request.args.get('days', default=7, type=int)
        limit = request.args.get('limit', default=100, type=int)

        changes = change_analyzer.get_recent_changes(days, limit)

        # Convert ObjectId to string
        for change in changes:
            change['_id'] = str(change['_id'])

        return jsonify({'changes': changes, 'count': len(changes)}), 200

    except Exception as e:
        logging.error(f"Get changes failed: {e}")
        return jsonify({'error': str(e)}), 500


@api.route('/analytics/recommendations', methods=['GET'])
def get_recommendations():
    """
    Get AI-generated recommendations
    GET /api/v1/analytics/recommendations
    """
    try:
        schema_collection = get_schema_collection()
        latest_schema = schema_collection.find_one({}, sort=[('version', -1)])

        if not latest_schema:
            return jsonify({'error': 'No schema found'}), 404

        schema = latest_schema.get('schema', {})
        stats = latest_schema.get('stats', {})

        recommendations = recommendation_engine.generate_recommendations(schema, stats)

        return jsonify({'recommendations': recommendations, 'count': len(recommendations)}), 200

    except Exception as e:
        logging.error(f"Get recommendations failed: {e}")
        return jsonify({'error': str(e)}), 500


@api.route('/data', methods=['GET'])
def get_data():
    """
    Query data from collection
    GET /api/v1/data?limit=<limit>&skip=<skip>&filter=<filter_json>
    """
    try:
        limit = request.args.get('limit', default=100, type=int)
        skip = request.args.get('skip', default=0, type=int)
        filter_json = request.args.get('filter', default='{}')

        import json
        filter_dict = json.loads(filter_json)

        collection = get_collection()
        data = list(collection.find(filter_dict).skip(skip).limit(limit))

        # Convert ObjectId to string
        for doc in data:
            doc['_id'] = str(doc['_id'])

        total_count = collection.count_documents(filter_dict)

        return jsonify({
            'data': data,
            'count': len(data),
            'total': total_count,
            'skip': skip,
            'limit': limit
        }), 200

    except Exception as e:
        logging.error(f"Get data failed: {e}")
        return jsonify({'error': str(e)}), 500


@api.route('/data/summary', methods=['GET'])
def get_summary():
    """
    Get summary statistics
    GET /api/v1/data/summary
    """
    try:
        schema_collection = get_schema_collection()
        latest_schema = schema_collection.find_one({}, sort=[('version', -1)])

        if not latest_schema:
            return jsonify({'error': 'No schema found'}), 404

        schema = latest_schema.get('schema', {})
        summary = data_insights.get_summary_statistics(schema)

        return jsonify(summary), 200

    except Exception as e:
        logging.error(f"Get summary failed: {e}")
        return jsonify({'error': str(e)}), 500


@api.route('/data/distribution/<field>', methods=['GET'])
def get_field_distribution(field):
    """
    Get value distribution for a field
    GET /api/v1/data/distribution/<field>?limit=<limit>
    """
    try:
        limit = request.args.get('limit', default=20, type=int)

        distribution = data_insights.get_field_distribution(field, limit)

        return jsonify(distribution), 200

    except Exception as e:
        logging.error(f"Get distribution failed: {e}")
        return jsonify({'error': str(e)}), 500


# Error handlers
@api.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404


@api.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500
