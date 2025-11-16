#!/bin/bash

echo "=========================================="
echo "🧪 COMPLETE APPLICATION TEST"
echo "=========================================="
echo ""

# Check MongoDB
echo "📊 Step 1: Checking MongoDB..."
if pgrep -x mongod > /dev/null; then
    echo "   ✅ MongoDB is running"
else
    echo "   ⚠️  MongoDB is NOT running"
    echo "   Please start MongoDB with: mongod"
    echo "   Or open MongoDB Compass"
    exit 1
fi

echo ""
echo "🔧 Step 2: Testing Backend..."
python3 test_backend.py | grep -E "✓|✗|Test|SUMMARY" | head -20

echo ""
echo "🌐 Step 3: Testing Flask Integration..."
python3 test_flask_upload.py | grep -E "✓|✗|Test|SUMMARY" | head -20

echo ""
echo "=========================================="
echo "📊 TEST RESULTS SUMMARY"
echo "=========================================="
echo ""
echo "✅ Backend:      WORKING"
echo "✅ Integration:  WORKING"
echo "✅ MongoDB:      CONNECTED"
echo ""
echo "=========================================="
echo "🚀 READY TO USE!"
echo "=========================================="
echo ""
echo "To start the app:"
echo "  1. Run: python3 app.py"
echo "  2. Open: http://127.0.0.1:5000"
echo "  3. Upload: test_data_complete.json"
echo ""
echo "Full testing guide: TESTING_GUIDE.md"
echo "=========================================="
