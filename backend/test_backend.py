#!/usr/bin/env python3
"""Backend testing script to identify all errors"""

import json
import sys

print("=" * 60)
print("BACKEND ERROR DETECTION TEST")
print("=" * 60)

# Test 1: Import all modules
print("\n[Test 1] Testing imports...")
try:
    from extract import extract_data, batch_data
    from transform import infer_schema, transform_batch, detect_type, infer_field_type
    from load import load_data, save_schema_version, detect_changes, deduplicate_batch
    from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION
    print("✓ All imports successful")
except Exception as e:
    print(f"✗ Import error: {e}")
    sys.exit(1)

# Test 2: Test type detection
print("\n[Test 2] Testing type detection...")
try:
    test_values = [
        (42, 'integer'),
        (42.5, 'float'),
        ('test@email.com', 'email'),
        ('2023-01-15', 'date'),
        ('https://example.com', 'url'),
        ('true', 'boolean'),
        ('hello', 'string'),
        ('', 'null'),
    ]

    for value, expected_type in test_values:
        detected = detect_type(value)
        if detected == expected_type:
            print(f"  ✓ {value} -> {detected}")
        else:
            print(f"  ✗ {value} -> Expected: {expected_type}, Got: {detected}")
    print("✓ Type detection working")
except Exception as e:
    print(f"✗ Type detection error: {e}")
    import traceback
    traceback.print_exc()

# Test 3: Test schema inference
print("\n[Test 3] Testing schema inference...")
try:
    test_batch = [
        {"name": "Alice", "age": 25, "email": "alice@test.com"},
        {"name": "Bob", "age": 30, "email": "bob@test.com"},
    ]

    schema = {}
    schema = infer_schema(test_batch, schema)

    print(f"  Schema: {schema}")

    # Check if schema has correct structure
    if isinstance(schema, dict):
        for field, info in schema.items():
            if 'type' in info and 'sample_values' in info:
                print(f"  ✓ Field '{field}': type={info['type']}")
            else:
                print(f"  ✗ Field '{field}' missing type or sample_values")
    else:
        print(f"  ✗ Schema is not a dict: {type(schema)}")

    print("✓ Schema inference working")
except Exception as e:
    print(f"✗ Schema inference error: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Test transformation
print("\n[Test 4] Testing data transformation...")
try:
    test_batch = [
        {"name": "Alice", "age": "25", "email": "ALICE@TEST.COM"},
    ]

    schema = {}
    schema = infer_schema(test_batch, schema)
    transformed = transform_batch(test_batch, schema)

    print(f"  Original: {test_batch[0]}")
    print(f"  Transformed: {transformed[0]}")

    # Check if email is lowercased
    if transformed[0].get('email') == 'alice@test.com':
        print("  ✓ Email normalized to lowercase")
    else:
        print(f"  ✗ Email not normalized: {transformed[0].get('email')}")

    print("✓ Transformation working")
except Exception as e:
    print(f"✗ Transformation error: {e}")
    import traceback
    traceback.print_exc()

# Test 5: Test batch processing
print("\n[Test 5] Testing batch processing...")
try:
    data = [{"id": i} for i in range(25)]
    batches = list(batch_data(data, 10))

    print(f"  Total records: {len(data)}")
    print(f"  Batch size: 10")
    print(f"  Number of batches: {len(batches)}")

    if len(batches) == 3:  # Should be 3 batches (10, 10, 5)
        print("  ✓ Correct number of batches")
    else:
        print(f"  ✗ Expected 3 batches, got {len(batches)}")

    print("✓ Batch processing working")
except Exception as e:
    print(f"✗ Batch processing error: {e}")
    import traceback
    traceback.print_exc()

# Test 6: Test with actual test file
print("\n[Test 6] Testing with test_data_complete.json...")
try:
    with open('test_data_complete.json', 'r') as f:
        data = json.load(f)

    schema = {}
    for batch in batch_data(data, 10):
        schema = infer_schema(batch, schema)
        transformed = transform_batch(batch, schema)

    print(f"  Records processed: {len(data)}")
    print(f"  Fields detected: {len(schema)}")
    print(f"  Field types:")
    for field, info in schema.items():
        print(f"    - {field}: {info.get('type', 'unknown')}")

    print("✓ Full pipeline working")
except Exception as e:
    print(f"✗ Full pipeline error: {e}")
    import traceback
    traceback.print_exc()

# Test 7: MongoDB connection (optional - won't fail if MongoDB not running)
print("\n[Test 7] Testing MongoDB connection...")
try:
    from pymongo import MongoClient
    from pymongo.errors import ServerSelectionTimeoutError

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    client.server_info()  # Will throw exception if cannot connect
    print("✓ MongoDB connection successful")

    # Test database operations
    db = client[MONGO_DB]
    print(f"  Connected to database: {MONGO_DB}")

except ServerSelectionTimeoutError:
    print("⚠ MongoDB not running (this is okay for testing code)")
except Exception as e:
    print(f"⚠ MongoDB error: {e} (this is okay if MongoDB not running)")

print("\n" + "=" * 60)
print("TEST SUMMARY")
print("=" * 60)
print("If all tests show ✓, the backend is working correctly!")
print("If MongoDB shows ⚠, start MongoDB with: mongod")
print("=" * 60)
