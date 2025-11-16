from flask import Flask, render_template, request
from flask_cors import CORS
import logging
from datetime import datetime
from bson import ObjectId
from extract import extract_data, batch_data
from transform import infer_schema, transform_batch
from load import load_data, save_schema_version
from config import BATCH_SIZE

# Import AI-powered modules
from ai_schema_inference import infer_schema_with_ai, ai_schema_inferencer
from ml_data_processing import process_data_with_ml
from analytics_engine import recommendation_engine
try:
    from api_routes_simple import api
except ImportError:
    # Create minimal API blueprint if import fails
    from flask import Blueprint
    api = Blueprint('api', __name__, url_prefix='/api/v1')

    @api.route('/health')
    def health():
        return {'status': 'ok'}

app = Flask(__name__)
CORS(app)  # Enable CORS for API access

# Register API blueprint
app.register_blueprint(api)

def sanitize_for_json(data):
    """Remove or convert MongoDB ObjectId and datetime fields for JSON serialization"""
    if isinstance(data, list):
        return [sanitize_for_json(item) for item in data]
    elif isinstance(data, dict):
        sanitized = {}
        for key, value in data.items():
            if key == '_id':
                # Skip _id field entirely
                continue
            elif isinstance(value, ObjectId):
                # Convert ObjectId to string
                sanitized[key] = str(value)
            elif isinstance(value, datetime):
                # Convert datetime to ISO format string
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

    if request.method == "POST":
        file = request.files["datafile"]
        data = extract_data(file)
        if not data:
            msg = "Invalid file format. Please upload JSON or CSV with records."
        else:
            total_duplicates = 0
            total_anomalies = 0

            for batch in batch_data(data, BATCH_SIZE):
                # Use AI-powered schema inference
                schema = infer_schema_with_ai(batch, schema)

                # Apply ML-powered data processing (missing value prediction, anomaly detection, enrichment)
                processed_batch, dup_count, anomaly_count = process_data_with_ml(batch, schema)
                total_duplicates += dup_count
                total_anomalies += anomaly_count

                # Transform and load
                t_batch = transform_batch(processed_batch, schema)
                changes, _ = load_data(t_batch)
                if changes:
                    all_changes.extend(changes)
                transformed = t_batch[:5]  # show sample of uploaded batch

            # Calculate statistics
            stats = {
                'total_records': len(data),
                'total_fields': len(schema),
                'fields_by_type': {},
                'semantic_categories': {},
                'changes_detected': len(all_changes),
                'duplicates_removed': total_duplicates,
                'anomalies_detected': total_anomalies,
                'inserted_records': len(data) - total_duplicates
            }

            # Count fields by type and semantic category
            for field_name, field_info in schema.items():
                field_type = field_info.get('type', 'unknown')
                semantic_cat = field_info.get('semantic_category', 'unknown')

                stats['fields_by_type'][field_type] = stats['fields_by_type'].get(field_type, 0) + 1
                stats['semantic_categories'][semantic_cat] = stats['semantic_categories'].get(semantic_cat, 0) + 1

            # Save schema version
            schema_version = save_schema_version(schema, stats)

            # Generate AI recommendations
            recommendations = recommendation_engine.generate_recommendations(schema, stats)

            change_msg = f" | {len(all_changes)} changes detected" if all_changes else ""
            dup_msg = f" | {total_duplicates} duplicates removed" if total_duplicates > 0 else ""
            anomaly_msg = f" | {total_anomalies} anomalies detected" if total_anomalies > 0 else ""
            rec_msg = f" | {len(recommendations)} recommendations" if recommendations else ""

            msg = f"🤖 AI-Processed {len(data)} records, inserted {stats['inserted_records']} with {len(schema)} fields! Schema v{schema_version} saved.{change_msg}{dup_msg}{anomaly_msg}{rec_msg}"

    # Sanitize data for JSON serialization (remove MongoDB ObjectId)
    transformed_clean = sanitize_for_json(transformed)
    changes_clean = sanitize_for_json(all_changes)

    return render_template("index.html", schema=schema, transformed=transformed_clean, msg=msg, stats=stats, changes=changes_clean, schema_version=schema_version)

if __name__ == "__main__":
    app.run(debug=True, port=5001)
