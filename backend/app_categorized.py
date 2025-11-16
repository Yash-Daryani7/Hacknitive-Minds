# app_categorized.py - Updated Flask App with Multi-Database Categorization

from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import logging
from datetime import datetime
from bson import ObjectId

# Import modules
from extract import extract_data, batch_data
from transform import infer_schema, transform_batch
from config_categorized import DatabaseConfig

# Import categorized loader
from load_categorized import CategorizedDataLoader

# Import AI-powered modules
from ai_schema_inference import infer_schema_with_ai, ai_schema_inferencer
from ml_data_processing import process_data_with_ml
from analytics_engine import recommendation_engine

try:
    from api_routes_simple import api
except ImportError:
    from flask import Blueprint
    api = Blueprint('api', __name__, url_prefix='/api/v1')

    @api.route('/health')
    def health():
        return {'status': 'ok'}

app = Flask(__name__)
CORS(app)

# Register API blueprint
app.register_blueprint(api)

# Initialize categorized loader
loader = CategorizedDataLoader(DatabaseConfig())

def sanitize_for_json(data):
    """Remove or convert MongoDB ObjectId and datetime fields for JSON serialization"""
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

logging.basicConfig(filename='logs/etl.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

@app.route("/", methods=["GET", "POST"])
def index():
    schema = {}
    transformed = []
    msg = ""
    stats = {}
    all_changes = []
    schema_version = None
    categorization_info = {}

    if request.method == "POST":
        file = request.files.get("datafile")

        # Get source and entity from form (optional - can auto-detect)
        source = request.form.get("source", "auto")
        entity = request.form.get("entity", "auto")

        if not file:
            msg = "❌ Please upload a file"
            return render_template("index_categorized.html",
                                 schema=schema,
                                 transformed=transformed,
                                 msg=msg,
                                 stats=stats,
                                 categorization_info=categorization_info)

        # Extract data
        data = extract_data(file)

        if not data:
            msg = "❌ Invalid file format. Please upload JSON, CSV, or PDF with records."
            return render_template("index_categorized.html",
                                 schema=schema,
                                 transformed=transformed,
                                 msg=msg,
                                 stats=stats,
                                 categorization_info=categorization_info)

        # Auto-detect source and entity if not specified
        if source == "auto" and data:
            source = loader.auto_detect_category(data[0])
            logging.info(f"Auto-detected source: {source}")

        if entity == "auto" and data:
            entity = loader.auto_detect_entity(data[0])
            logging.info(f"Auto-detected entity: {entity}")

        # Initialize stats
        total_inserted = 0
        total_duplicates = 0
        total_anomalies = 0
        total_changes = 0
        all_versions = []

        # Process in batches
        for batch in batch_data(data, DatabaseConfig.BATCH_SIZE):
            # Use AI-powered schema inference
            schema = infer_schema_with_ai(batch, schema)

            # Apply ML-powered data processing
            processed_batch, dup_count, anomaly_count = process_data_with_ml(batch, schema)
            total_anomalies += anomaly_count

            # Transform batch
            t_batch = transform_batch(processed_batch, schema)

            # Load with categorization and versioning
            load_result = loader.load_categorized_data(
                t_batch,
                source=source,
                entity=entity,
                schema=schema,
                deduplicate=True,
                detect_change=True
            )

            # Accumulate stats
            total_inserted += load_result['inserted_count']
            total_duplicates += load_result['duplicate_count']
            total_changes += load_result['change_count']

            if load_result['changes']:
                all_changes.extend(load_result['changes'])

            # Track version
            if load_result['version'] not in all_versions:
                all_versions.append(load_result['version'])

            schema_version = load_result['version']

            # Store categorization info
            categorization_info = {
                'source': load_result['source'],
                'entity': load_result['entity'],
                'database': load_result['database'],
                'collection': load_result['collection'],
                'version': load_result['version'],
                'is_new_version': load_result['is_new_version']
            }

            # Sample transformed data
            transformed = t_batch[:5]

        # Calculate statistics
        stats = {
            'total_records': len(data),
            'total_fields': len(schema),
            'fields_by_type': {},
            'semantic_categories': {},
            'changes_detected': total_changes,
            'duplicates_removed': total_duplicates,
            'anomalies_detected': total_anomalies,
            'inserted_records': total_inserted,
            'versions': all_versions,
        }

        # Count fields by type and semantic category
        for field_name, field_info in schema.items():
            field_type = field_info.get('type', 'unknown')
            semantic_cat = field_info.get('semantic_category', 'unknown')

            stats['fields_by_type'][field_type] = stats['fields_by_type'].get(field_type, 0) + 1
            stats['semantic_categories'][semantic_cat] = stats['semantic_categories'].get(semantic_cat, 0) + 1

        # Generate AI recommendations
        recommendations = recommendation_engine.generate_recommendations(schema, stats)

        # Build success message
        change_msg = f" | {total_changes} changes detected" if total_changes > 0 else ""
        dup_msg = f" | {total_duplicates} duplicates removed" if total_duplicates > 0 else ""
        anomaly_msg = f" | {total_anomalies} anomalies detected" if total_anomalies > 0 else ""
        rec_msg = f" | {len(recommendations)} recommendations" if recommendations else ""
        version_msg = f" | Schema v{schema_version}" if schema_version else ""

        msg = (f"🎯 Loaded {total_inserted} records to "
               f"{categorization_info['database']}.{categorization_info['collection']}"
               f"{version_msg}{change_msg}{dup_msg}{anomaly_msg}{rec_msg}")

        logging.info(msg)

    # Sanitize data for JSON serialization
    transformed_clean = sanitize_for_json(transformed)
    changes_clean = sanitize_for_json(all_changes)
    categorization_clean = sanitize_for_json(categorization_info)

    return render_template(
        "index_categorized.html",
        schema=schema,
        transformed=transformed_clean,
        msg=msg,
        stats=stats,
        changes=changes_clean,
        schema_version=schema_version,
        categorization=categorization_clean
    )

@app.route("/api/categorization/stats", methods=["GET"])
def get_categorization_stats():
    """API endpoint to get database categorization statistics"""
    try:
        stats = loader.get_database_stats()
        return jsonify({
            "status": "success",
            "data": stats
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/api/schema/history", methods=["GET"])
def get_schema_history():
    """API endpoint to get schema evolution history"""
    source = request.args.get("source")
    entity = request.args.get("entity")

    if not source or not entity:
        return jsonify({
            "status": "error",
            "message": "source and entity parameters required"
        }), 400

    try:
        history = loader.get_schema_history(source, entity)
        return jsonify({
            "status": "success",
            "data": sanitize_for_json(history)
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/api/query/across_versions", methods=["POST"])
def query_across_versions():
    """API endpoint to query data across all schema versions"""
    data = request.get_json()

    source = data.get("source")
    entity = data.get("entity")
    query = data.get("query", {})
    limit = data.get("limit", 100)

    if not source or not entity:
        return jsonify({
            "status": "error",
            "message": "source and entity required"
        }), 400

    try:
        results = loader.query_across_versions(source, entity, query, limit)
        return jsonify({
            "status": "success",
            "count": len(results),
            "data": sanitize_for_json(results)
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/api/sources", methods=["GET"])
def get_available_sources():
    """Get list of available data sources"""
    return jsonify({
        "status": "success",
        "sources": list(DatabaseConfig.SOURCE_DB_MAPPING.keys())
    })

if __name__ == "__main__":
    try:
        app.run(debug=True, port=5001, host='0.0.0.0')
    finally:
        loader.close()
