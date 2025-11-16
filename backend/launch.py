#!/usr/bin/env python3
"""
Simple launcher for Dynamic ETL Pipeline
Works with or without Ollama
"""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("="*70)
print("🚀 Dynamic ETL Pipeline - AI-Powered Data Categorization")
print("="*70)
print()

# Try to use the categorized app first
try:
    print("📦 Loading categorized app with AI features...")
    from flask import Flask
    from flask_cors import CORS

    app = Flask(__name__)
    CORS(app)

    # Import modules
    from extract import extract_data, batch_data
    from transform import infer_schema, transform_batch
    from config import BATCH_SIZE

    # Try to import AI modules (optional)
    try:
        from ai_schema_inference import infer_schema_with_ai
        from ml_data_processing import process_data_with_ml
        from analytics_engine import recommendation_engine
        print("✅ AI modules loaded")
        USE_AI = True
    except ImportError as e:
        print(f"⚠️  AI modules not available: {e}")
        print("ℹ️  Using basic features (AI disabled)")
        USE_AI = False

    # Try to import categorized loader (optional)
    try:
        from load_categorized import CategorizedDataLoader
        from config_categorized import DatabaseConfig
        loader = CategorizedDataLoader(DatabaseConfig())
        print("✅ Multi-database categorization enabled")
        USE_CATEGORIZED = True
    except ImportError as e:
        print(f"⚠️  Categorized loader not available: {e}")
        print("ℹ️  Using single database mode")
        from load import load_data, save_schema_version
        USE_CATEGORIZED = False

    from datetime import datetime
    from bson import ObjectId

    def sanitize_for_json(data):
        """Remove or convert MongoDB ObjectId and datetime fields"""
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

    @app.route("/", methods=["GET", "POST"])
    def index():
        from flask import render_template, request

        schema = {}
        transformed = []
        msg = ""
        stats = {}
        all_changes = []
        schema_version = None
        categorization_info = {}

        if request.method == "POST":
            file = request.files.get("datafile")

            if not file:
                msg = "❌ Please upload a file"
                return render_template("index.html",
                                     schema=schema,
                                     transformed=transformed,
                                     msg=msg,
                                     stats=stats)

            # Extract data
            data = extract_data(file)

            if not data:
                msg = "❌ Invalid file format. Please upload JSON or CSV."
                return render_template("index.html",
                                     schema=schema,
                                     transformed=transformed,
                                     msg=msg,
                                     stats=stats)

            # Process data
            total_inserted = 0
            total_duplicates = 0

            for batch in batch_data(data, BATCH_SIZE):
                # Use AI if available, otherwise basic
                if USE_AI:
                    schema = infer_schema_with_ai(batch, schema)
                    processed_batch, dup_count, anomaly_count = process_data_with_ml(batch, schema)
                else:
                    schema = infer_schema(batch, schema)
                    processed_batch = batch
                    dup_count = 0

                # Transform
                t_batch = transform_batch(processed_batch, schema)

                # Load (categorized or simple)
                if USE_CATEGORIZED:
                    source = request.form.get("source", "auto")
                    entity = request.form.get("entity", "auto")

                    if source == "auto" and data:
                        source = loader.auto_detect_category(data[0])
                    if entity == "auto" and data:
                        entity = loader.auto_detect_entity(data[0])

                    result = loader.load_categorized_data(
                        t_batch,
                        source=source,
                        entity=entity,
                        schema=schema
                    )
                    total_inserted += result['inserted_count']
                    total_duplicates += result['duplicate_count']
                    schema_version = result['version']
                    categorization_info = {
                        'source': result['source'],
                        'entity': result['entity'],
                        'database': result['database'],
                        'collection': result['collection'],
                        'version': result['version']
                    }
                else:
                    changes, dups = load_data(t_batch)
                    total_inserted += len(t_batch) - dups
                    total_duplicates += dups
                    if changes:
                        all_changes.extend(changes)
                    schema_version = save_schema_version(schema, {})

                transformed = t_batch[:5]

            # Stats
            stats = {
                'total_records': len(data),
                'total_fields': len(schema),
                'inserted_records': total_inserted,
                'duplicates_removed': total_duplicates,
                'changes_detected': len(all_changes)
            }

            msg = f"✅ Processed {len(data)} records, inserted {total_inserted}"
            if total_duplicates > 0:
                msg += f", skipped {total_duplicates} duplicates"

        # Sanitize
        transformed_clean = sanitize_for_json(transformed)
        changes_clean = sanitize_for_json(all_changes)

        # Use categorized template if available
        template = "index_categorized.html" if USE_CATEGORIZED else "index.html"

        return render_template(
            template,
            schema=schema,
            transformed=transformed_clean,
            msg=msg,
            stats=stats,
            changes=changes_clean,
            schema_version=schema_version,
            categorization=categorization_info
        )

    print()
    print("="*70)
    print("✅ App loaded successfully!")
    print("="*70)
    print()
    print("🌐 Open your browser and go to:")
    print()
    print("   👉  http://localhost:5001")
    print()
    print("="*70)
    print()
    print("⏹  Press CTRL+C to stop")
    print()

    # Run app
    app.run(host='0.0.0.0', port=5001, debug=True, use_reloader=False)

except Exception as e:
    print(f"❌ Error starting app: {e}")
    print()
    print("💡 Solution:")
    print("   pip3 install flask flask-cors pymongo")
    import traceback
    traceback.print_exc()
    sys.exit(1)
