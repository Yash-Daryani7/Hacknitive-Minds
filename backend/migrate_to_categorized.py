#!/usr/bin/env python3
# migrate_to_categorized.py - Migrate existing data to categorized structure

"""
Migration Script for Multi-Database Categorization

This script helps you migrate existing data from the old single-database
structure to the new multi-database categorized structure with versioning.

Usage:
    python migrate_to_categorized.py [--dry-run] [--source SOURCE] [--entity ENTITY]

Options:
    --dry-run    Show what would be migrated without actually doing it
    --source     Manually specify source category (otherwise auto-detect)
    --entity     Manually specify entity type (otherwise auto-detect)
    --batch-size Batch size for migration (default: 1000)
"""

import argparse
import logging
from pymongo import MongoClient
from datetime import datetime
from tqdm import tqdm
from config_categorized import DatabaseConfig
from load_categorized import CategorizedDataLoader
from ai_schema_inference import infer_schema_with_ai

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataMigrator:
    """Migrate data from old structure to categorized structure"""

    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.config = DatabaseConfig()
        self.client = MongoClient(self.config.MONGO_URI)
        self.loader = CategorizedDataLoader(self.config)

        logger.info(f"Migration mode: {'DRY RUN' if dry_run else 'LIVE'}")

    def get_old_data(self):
        """Get data from old single-database structure"""
        try:
            # Try old database structure
            old_db = self.client["hackathon_db"]
            old_collection = old_db["entries"]

            count = old_collection.count_documents({})
            logger.info(f"Found {count} records in old structure")

            return old_collection, count

        except Exception as e:
            logger.error(f"Error accessing old data: {e}")
            return None, 0

    def analyze_data(self, collection, limit=100):
        """Analyze sample of data to determine categorization"""
        logger.info("Analyzing sample data for categorization...")

        sample = list(collection.find().limit(limit))

        if not sample:
            logger.warning("No data found to analyze")
            return None, None

        # Auto-detect source and entity
        first_record = sample[0]
        detected_source = self.loader.auto_detect_category(first_record)
        detected_entity = self.loader.auto_detect_entity(first_record)

        logger.info(f"Auto-detected source: {detected_source}")
        logger.info(f"Auto-detected entity: {detected_entity}")

        # Infer schema from sample
        schema = {}
        for record in sample:
            schema = infer_schema_with_ai([record], schema)

        logger.info(f"Inferred schema with {len(schema)} fields")

        return {
            'source': detected_source,
            'entity': detected_entity,
            'schema': schema,
            'sample_size': len(sample)
        }, sample

    def migrate_data(
        self,
        source_collection,
        total_count,
        source_category,
        entity_type,
        batch_size=1000
    ):
        """Migrate data in batches"""
        logger.info(f"Starting migration to {source_category}.{entity_type}...")

        migrated_count = 0
        error_count = 0
        schema = {}

        # Create progress bar
        with tqdm(total=total_count, desc="Migrating") as pbar:
            cursor = source_collection.find().batch_size(batch_size)

            batch = []
            for record in cursor:
                # Remove old _id and _loaded_at if present
                if '_id' in record:
                    del record['_id']
                if '_loaded_at' in record:
                    del record['_loaded_at']

                batch.append(record)

                # Process batch when full
                if len(batch) >= batch_size:
                    try:
                        if not self.dry_run:
                            # Infer schema for this batch
                            schema = infer_schema_with_ai(batch, schema)

                            # Load with categorization
                            result = self.loader.load_categorized_data(
                                batch,
                                source=source_category,
                                entity=entity_type,
                                schema=schema,
                                deduplicate=False,  # Don't deduplicate during migration
                                detect_change=False  # Don't detect changes during migration
                            )

                            migrated_count += result['inserted_count']
                        else:
                            migrated_count += len(batch)

                        pbar.update(len(batch))
                        batch = []

                    except Exception as e:
                        logger.error(f"Error migrating batch: {e}")
                        error_count += len(batch)
                        batch = []

            # Process remaining records
            if batch:
                try:
                    if not self.dry_run:
                        schema = infer_schema_with_ai(batch, schema)

                        result = self.loader.load_categorized_data(
                            batch,
                            source=source_category,
                            entity=entity_type,
                            schema=schema,
                            deduplicate=False,
                            detect_change=False
                        )

                        migrated_count += result['inserted_count']
                    else:
                        migrated_count += len(batch)

                    pbar.update(len(batch))

                except Exception as e:
                    logger.error(f"Error migrating final batch: {e}")
                    error_count += len(batch)

        return migrated_count, error_count

    def run_migration(
        self,
        manual_source=None,
        manual_entity=None,
        batch_size=1000
    ):
        """Run complete migration process"""
        logger.info("="*60)
        logger.info("DATA MIGRATION TO CATEGORIZED STRUCTURE")
        logger.info("="*60)

        # Get old data
        old_collection, total_count = self.get_old_data()

        if not old_collection or total_count == 0:
            logger.warning("No data to migrate")
            return

        # Analyze data
        analysis, sample = self.analyze_data(old_collection)

        if not analysis:
            logger.error("Failed to analyze data")
            return

        # Use manual categorization if provided
        source_category = manual_source or analysis['source']
        entity_type = manual_entity or analysis['entity']

        logger.info(f"\nMigration Plan:")
        logger.info(f"  Source Category: {source_category}")
        logger.info(f"  Entity Type: {entity_type}")
        logger.info(f"  Total Records: {total_count}")
        logger.info(f"  Target Database: {self.config.get_db_for_source(source_category)}")
        logger.info(f"  Batch Size: {batch_size}")
        logger.info(f"  Mode: {'DRY RUN' if self.dry_run else 'LIVE MIGRATION'}")

        if self.dry_run:
            logger.info("\n⚠️  DRY RUN MODE - No data will be modified")
            logger.info(f"Would migrate {total_count} records to:")
            logger.info(f"  Database: {self.config.get_db_for_source(source_category)}")
            logger.info(f"  Collection Pattern: {source_category}_{entity_type}_v*")
            return

        # Confirm migration
        print("\n" + "="*60)
        response = input("Proceed with migration? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Migration cancelled")
            return

        # Perform migration
        logger.info("\nStarting migration...")
        start_time = datetime.now()

        migrated, errors = self.migrate_data(
            old_collection,
            total_count,
            source_category,
            entity_type,
            batch_size
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Report results
        logger.info("\n" + "="*60)
        logger.info("MIGRATION COMPLETE")
        logger.info("="*60)
        logger.info(f"Total Records: {total_count}")
        logger.info(f"Successfully Migrated: {migrated}")
        logger.info(f"Errors: {errors}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Speed: {migrated/duration:.2f} records/second")
        logger.info("="*60)

        # Get final stats
        stats = self.loader.get_database_stats()
        logger.info(f"\nDatabase Statistics:")
        for db_name, db_stats in stats['databases'].items():
            if db_stats['records'] > 0:
                logger.info(f"  {db_name}: {db_stats['records']} records in {db_stats['collections']} collections")

    def close(self):
        """Close connections"""
        self.loader.close()
        self.client.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Migrate data to categorized multi-database structure"
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Show what would be migrated without actually doing it"
    )

    parser.add_argument(
        '--source',
        type=str,
        help="Manually specify source category (otherwise auto-detect)"
    )

    parser.add_argument(
        '--entity',
        type=str,
        help="Manually specify entity type (otherwise auto-detect)"
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help="Batch size for migration (default: 1000)"
    )

    args = parser.parse_args()

    # Run migration
    migrator = DataMigrator(dry_run=args.dry_run)

    try:
        migrator.run_migration(
            manual_source=args.source,
            manual_entity=args.entity,
            batch_size=args.batch_size
        )
    except KeyboardInterrupt:
        logger.info("\nMigration interrupted by user")
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
    finally:
        migrator.close()


if __name__ == "__main__":
    main()
