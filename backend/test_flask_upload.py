#!/usr/bin/env python3
"""Test Flask application with file upload simulation"""

import io
import json

print("=" * 60)
print("FLASK APP INTEGRATION TEST")
print("=" * 60)

# Import Flask app
from app import app

# Create test client
app.config['TESTING'] = True
client = app.test_client()

# Test 1: GET homepage
print("\n[Test 1] Testing GET homepage...")
try:
    response = client.get('/')
    if response.status_code == 200:
        print(f"✓ Homepage loads successfully (status: {response.status_code})")
    else:
        print(f"✗ Homepage failed (status: {response.status_code})")
except Exception as e:
    print(f"✗ Homepage error: {e}")
    import traceback
    traceback.print_exc()

# Test 2: POST with JSON file
print("\n[Test 2] Testing POST with JSON file...")
try:
    # Load test data
    with open('test_data_complete.json', 'r') as f:
        test_data = f.read()

    # Create file upload
    data = {
        'datafile': (io.BytesIO(test_data.encode()), 'test.json')
    }

    response = client.post('/', data=data, content_type='multipart/form-data')

    if response.status_code == 200:
        print(f"✓ JSON upload successful (status: {response.status_code})")

        # Check if response contains expected text
        response_text = response.data.decode()
        if 'Schema' in response_text:
            print("  ✓ Schema detected in response")
        if 'Records' in response_text or 'records' in response_text:
            print("  ✓ Records processed")
        if 'Schema v' in response_text or 'version' in response_text:
            print("  ✓ Schema versioning working")
    else:
        print(f"✗ JSON upload failed (status: {response.status_code})")
        print(f"  Response: {response.data.decode()[:500]}")

except Exception as e:
    print(f"✗ JSON upload error: {e}")
    import traceback
    traceback.print_exc()

# Test 3: POST with invalid file
print("\n[Test 3] Testing POST with invalid file...")
try:
    data = {
        'datafile': (io.BytesIO(b'invalid data'), 'test.txt')
    }

    response = client.post('/', data=data, content_type='multipart/form-data')

    if response.status_code == 200:
        response_text = response.data.decode()
        if 'Invalid' in response_text or 'error' in response_text.lower():
            print("✓ Invalid file handled correctly")
        else:
            print("⚠ Invalid file may not be handled properly")
    else:
        print(f"✗ Error handling failed (status: {response.status_code})")

except Exception as e:
    print(f"✗ Invalid file test error: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Test deduplication
print("\n[Test 4] Testing deduplication (upload same file twice)...")
try:
    with open('test_data_complete.json', 'r') as f:
        test_data = f.read()

    # First upload
    data = {
        'datafile': (io.BytesIO(test_data.encode()), 'test1.json')
    }
    response1 = client.post('/', data=data, content_type='multipart/form-data')

    # Second upload (duplicates)
    data = {
        'datafile': (io.BytesIO(test_data.encode()), 'test2.json')
    }
    response2 = client.post('/', data=data, content_type='multipart/form-data')

    response_text = response2.data.decode()

    if 'duplicate' in response_text.lower() or 'skipped' in response_text.lower():
        print("✓ Deduplication working")
    else:
        print("⚠ Deduplication may not be working (check MongoDB manually)")

except Exception as e:
    print(f"✗ Deduplication test error: {e}")
    import traceback
    traceback.print_exc()

# Test 5: Test change detection
print("\n[Test 5] Testing change detection...")
try:
    # Upload original data
    with open('test_data_complete.json', 'r') as f:
        test_data = f.read()

    data = {
        'datafile': (io.BytesIO(test_data.encode()), 'original.json')
    }
    response1 = client.post('/', data=data, content_type='multipart/form-data')

    # Upload modified data
    with open('test_data_modified.json', 'r') as f:
        modified_data = f.read()

    data = {
        'datafile': (io.BytesIO(modified_data.encode()), 'modified.json')
    }
    response2 = client.post('/', data=data, content_type='multipart/form-data')

    response_text = response2.data.decode()

    if 'change' in response_text.lower():
        print("✓ Change detection working")
    else:
        print("⚠ Change detection may not be showing (check MongoDB changes collection)")

except Exception as e:
    print(f"✗ Change detection test error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("INTEGRATION TEST SUMMARY")
print("=" * 60)
print("✓ = Working correctly")
print("⚠ = May need manual verification")
print("✗ = Error detected")
print("=" * 60)
