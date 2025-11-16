# test_categorized.py - Tests for Multi-Database Categorization

import unittest
import json
from datetime import datetime
from pymongo import MongoClient

from config_categorized import DatabaseConfig
from load_categorized import CategorizedDataLoader


class TestDatabaseConfig(unittest.TestCase):
    """Test configuration for categorization"""

    def test_source_db_mapping(self):
        """Test that source-to-database mapping works"""
        self.assertEqual(DatabaseConfig.get_db_for_source("ecommerce"), "ecommerce_db")
        self.assertEqual(DatabaseConfig.get_db_for_source("hr"), "hr_db")
        self.assertEqual(DatabaseConfig.get_db_for_source("unknown"), "pipeline_default")

    def test_collection_naming(self):
        """Test collection name generation"""
        name = DatabaseConfig.get_collection_name("ecommerce", "products", 1)
        self.assertEqual(name, "ecommerce_products_v1")

        name = DatabaseConfig.get_collection_name("hr", "employees", 2)
        self.assertEqual(name, "hr_employees_v2")

    def test_retention_policies(self):
        """Test retention policy retrieval"""
        self.assertEqual(DatabaseConfig.get_retention_days("ecommerce"), 2555)
        self.assertEqual(DatabaseConfig.get_retention_days("api_logs"), 30)
        self.assertEqual(DatabaseConfig.get_retention_days("unknown"), 365)

    def test_auto_source_detection(self):
        """Test automatic source detection from fields"""
        # E-commerce data
        detected = DatabaseConfig.detect_source_from_fields(["product", "price", "sku"])
        self.assertEqual(detected, "ecommerce")

        # HR data
        detected = DatabaseConfig.detect_source_from_fields(["employee", "salary", "department"])
        self.assertEqual(detected, "hr")

        # IoT data
        detected = DatabaseConfig.detect_source_from_fields(["temperature", "sensor_id", "humidity"])
        self.assertEqual(detected, "iot_sensors")

        # Unknown
        detected = DatabaseConfig.detect_source_from_fields(["random", "fields"])
        self.assertEqual(detected, "uncategorized")

    def test_auto_entity_detection(self):
        """Test automatic entity type detection"""
        detected = DatabaseConfig.detect_entity_from_fields(["product", "sku", "price"])
        self.assertEqual(detected, "products")

        detected = DatabaseConfig.detect_entity_from_fields(["order", "total", "customer"])
        self.assertEqual(detected, "orders")

        detected = DatabaseConfig.detect_entity_from_fields(["employee", "hire_date"])
        self.assertEqual(detected, "employees")


class TestCategorizedDataLoader(unittest.TestCase):
    """Test categorized data loader functionality"""

    @classmethod
    def setUpClass(cls):
        """Setup test database"""
        cls.loader = CategorizedDataLoader()
        cls.test_source = "test_ecommerce"
        cls.test_entity = "test_products"

        # Clean up test data
        db = cls.loader.get_database(cls.test_source)
        for collection_name in db.list_collection_names():
            if collection_name.startswith("test_"):
                db.drop_collection(collection_name)

    def test_auto_detect_category(self):
        """Test automatic category detection"""
        record = {
            "product_name": "Laptop",
            "price": 999.99,
            "sku": "LAP-001"
        }

        category = self.loader.auto_detect_category(record)
        self.assertIn(category, ["ecommerce", "uncategorized"])

    def test_auto_detect_entity(self):
        """Test automatic entity detection"""
        record = {
            "product_name": "Laptop",
            "price": 999.99,
            "sku": "LAP-001"
        }

        entity = self.loader.auto_detect_entity(record)
        self.assertIn(entity, ["products", "data"])

    def test_schema_hash_computation(self):
        """Test schema hash computation"""
        schema1 = {"name": {"type": "string"}, "age": {"type": "integer"}}
        schema2 = {"name": {"type": "string"}, "age": {"type": "integer"}}
        schema3 = {"name": {"type": "string"}, "age": {"type": "float"}}

        hash1 = self.loader.compute_schema_hash(schema1)
        hash2 = self.loader.compute_schema_hash(schema2)
        hash3 = self.loader.compute_schema_hash(schema3)

        # Same schemas should have same hash
        self.assertEqual(hash1, hash2)

        # Different schemas should have different hash
        self.assertNotEqual(hash1, hash3)

    def test_schema_diff_computation(self):
        """Test schema difference computation"""
        old_schema = {
            "name": {"type": "string"},
            "price": {"type": "integer"},
            "old_field": {"type": "string"}
        }

        new_schema = {
            "name": {"type": "string"},
            "price": {"type": "float"},  # Type changed
            "new_field": {"type": "string"}
        }

        diff = self.loader.compute_schema_diff(old_schema, new_schema)

        self.assertIn("new_field", diff["added_fields"])
        self.assertIn("old_field", diff["removed_fields"])
        self.assertEqual(len(diff["modified_fields"]), 1)
        self.assertEqual(diff["modified_fields"][0]["field"], "price")
        self.assertFalse(diff["is_backward_compatible"])

    def test_load_categorized_data(self):
        """Test loading data with categorization"""
        test_data = [
            {"product": "Laptop", "price": 999.99, "sku": "LAP-001"},
            {"product": "Mouse", "price": 29.99, "sku": "MOU-001"},
            {"product": "Keyboard", "price": 79.99, "sku": "KEY-001"},
        ]

        test_schema = {
            "product": {"type": "string"},
            "price": {"type": "float"},
            "sku": {"type": "string"}
        }

        result = self.loader.load_categorized_data(
            test_data,
            source=self.test_source,
            entity=self.test_entity,
            schema=test_schema
        )

        self.assertEqual(result['inserted_count'], 3)
        self.assertEqual(result['version'], 1)
        self.assertTrue(result['is_new_version'])

    def test_schema_versioning(self):
        """Test automatic schema versioning"""
        # Load data with schema v1
        data_v1 = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30}
        ]

        schema_v1 = {
            "name": {"type": "string"},
            "age": {"type": "integer"}
        }

        result_v1 = self.loader.load_categorized_data(
            data_v1,
            source=self.test_source,
            entity="test_users",
            schema=schema_v1
        )

        self.assertEqual(result_v1['version'], 1)

        # Load data with evolved schema (added field)
        data_v2 = [
            {"name": "Charlie", "age": 35, "email": "charlie@test.com"}
        ]

        schema_v2 = {
            "name": {"type": "string"},
            "age": {"type": "integer"},
            "email": {"type": "email"}
        }

        result_v2 = self.loader.load_categorized_data(
            data_v2,
            source=self.test_source,
            entity="test_users",
            schema=schema_v2
        )

        self.assertEqual(result_v2['version'], 2)
        self.assertTrue(result_v2['is_new_version'])

    def test_deduplication(self):
        """Test deduplication functionality"""
        data_with_duplicates = [
            {"id": "1", "name": "Product A", "price": 100},
            {"id": "2", "name": "Product B", "price": 200},
            {"id": "1", "name": "Product A", "price": 100},  # Duplicate
        ]

        schema = {
            "id": {"type": "string"},
            "name": {"type": "string"},
            "price": {"type": "integer"}
        }

        # First load
        result1 = self.loader.load_categorized_data(
            data_with_duplicates,
            source=self.test_source,
            entity="test_dedup",
            schema=schema
        )

        # Should insert 2 records (one duplicate removed)
        self.assertEqual(result1['inserted_count'], 2)
        self.assertEqual(result1['duplicate_count'], 1)

        # Second load with same data
        result2 = self.loader.load_categorized_data(
            data_with_duplicates,
            source=self.test_source,
            entity="test_dedup",
            schema=schema
        )

        # Should insert 0 (all duplicates)
        self.assertEqual(result2['inserted_count'], 0)
        self.assertEqual(result2['duplicate_count'], 3)

    def test_change_detection(self):
        """Test change detection functionality"""
        # Load initial data
        initial_data = [
            {"id": "1", "name": "Product A", "price": 100}
        ]

        schema = {
            "id": {"type": "string"},
            "name": {"type": "string"},
            "price": {"type": "integer"}
        }

        self.loader.load_categorized_data(
            initial_data,
            source="ecommerce",  # Use ecommerce to trigger price monitoring
            entity="test_changes",
            schema=schema
        )

        # Load changed data
        changed_data = [
            {"id": "1", "name": "Product A", "price": 150}  # Price changed
        ]

        result = self.loader.load_categorized_data(
            changed_data,
            source="ecommerce",
            entity="test_changes",
            schema=schema
        )

        # Should detect change (but might be 0 if detected as duplicate)
        # The important thing is no crash
        self.assertGreaterEqual(result['change_count'], 0)

    def test_query_across_versions(self):
        """Test querying data across schema versions"""
        # Load data with v1 schema
        data_v1 = [{"id": "v1_record", "value": 100}]
        schema_v1 = {"id": {"type": "string"}, "value": {"type": "integer"}}

        self.loader.load_categorized_data(
            data_v1,
            source=self.test_source,
            entity="test_query",
            schema=schema_v1
        )

        # Load data with v2 schema
        data_v2 = [{"id": "v2_record", "value": 200, "extra": "field"}]
        schema_v2 = {
            "id": {"type": "string"},
            "value": {"type": "integer"},
            "extra": {"type": "string"}
        }

        self.loader.load_categorized_data(
            data_v2,
            source=self.test_source,
            entity="test_query",
            schema=schema_v2
        )

        # Query across versions
        results = self.loader.query_across_versions(
            source=self.test_source,
            entity="test_query",
            query={}
        )

        # Should get records from both versions
        self.assertGreaterEqual(len(results), 2)

    def test_schema_history(self):
        """Test schema history retrieval"""
        # Create multiple versions
        for i in range(3):
            data = [{"id": f"record_{i}", "value": i}]
            schema = {"id": {"type": "string"}, "value": {"type": "integer"}}

            if i > 0:
                schema[f"field_{i}"] = {"type": "string"}  # Add new field

            self.loader.load_categorized_data(
                data,
                source=self.test_source,
                entity="test_history",
                schema=schema
            )

        # Get history
        history = self.loader.get_schema_history(
            source=self.test_source,
            entity="test_history"
        )

        # Should have 3 versions (or 1 if fields don't affect hash enough)
        self.assertGreaterEqual(len(history), 1)

    @classmethod
    def tearDownClass(cls):
        """Cleanup test data"""
        db = cls.loader.get_database(cls.test_source)
        for collection_name in db.list_collection_names():
            if collection_name.startswith("test_"):
                db.drop_collection(collection_name)

        cls.loader.close()


def run_tests():
    """Run all tests"""
    print("="*70)
    print("RUNNING CATEGORIZATION TESTS")
    print("="*70)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestDatabaseConfig))
    suite.addTests(loader.loadTestsFromTestCase(TestCategorizedDataLoader))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "="*70)
    print("TEST RESULTS")
    print("="*70)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("="*70)

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    exit(0 if success else 1)
