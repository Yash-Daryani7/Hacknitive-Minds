#!/usr/bin/env python3
"""
Simple server startup script
"""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("🚀 Starting AI-Powered Data Pipeline...")
print("=" * 60)

# Import and run app
try:
    from app import app

    print("✅ Flask app loaded")
    print("📡 Server starting on http://localhost:5001")
    print("=" * 60)
    print("\n🌐 Open your browser and go to:")
    print("   👉 http://localhost:5001")
    print("\n📌 API Endpoints:")
    print("   POST http://localhost:5001/api/v1/ingest")
    print("   GET  http://localhost:5001/api/v1/schema")
    print("   GET  http://localhost:5001/api/v1/health")
    print("\n⏹  Press CTRL+C to stop\n")
    print("=" * 60)

    # Run the app
    app.run(host='0.0.0.0', port=5001, debug=True, use_reloader=False)

except ImportError as e:
    print(f"❌ Import Error: {e}")
    print("\n💡 Solution: Install missing dependencies:")
    print("   pip3 install -r requirements.txt")
    sys.exit(1)

except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)
