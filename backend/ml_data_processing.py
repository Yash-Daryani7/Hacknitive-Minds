"""
ML-Powered Data Processing Module
Includes missing value prediction, anomaly detection, and data enrichment
"""

import logging
import re
from collections import defaultdict, Counter
import numpy as np
from datetime import datetime

try:
    from sklearn.impute import KNNImputer, SimpleImputer
    from sklearn.ensemble import IsolationForest, RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    import pandas as pd
    from email_validator import validate_email, EmailNotValidError
    import phonenumbers
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False
    logging.warning("ML dependencies not available. Install requirements.txt")


class MissingValuePredictor:
    """Predict and fill missing values using ML"""

    def __init__(self):
        self.imputers = {}
        self.label_encoders = {}

    def predict_missing_values(self, data, schema):
        """
        Intelligently predict missing values based on field type and semantic category
        """
        if not ML_AVAILABLE:
            logging.warning("ML libraries not available, using simple imputation")
            return self._simple_imputation(data, schema)

        try:
            df = pd.DataFrame(data)

            # Separate numeric and categorical columns
            numeric_cols = []
            categorical_cols = []

            for field, field_info in schema.items():
                if field not in df.columns:
                    continue

                field_type = field_info.get('type', 'string')
                semantic_cat = field_info.get('semantic_category', 'unknown')

                if field_type in ['integer', 'float']:
                    numeric_cols.append(field)
                else:
                    categorical_cols.append(field)

            # Handle numeric columns with KNN imputation
            if numeric_cols:
                numeric_data = df[numeric_cols].copy()

                # Convert to numeric, coerce errors
                for col in numeric_cols:
                    numeric_data[col] = pd.to_numeric(numeric_data[col], errors='coerce')

                # Check if there are any missing values
                if numeric_data.isnull().any().any():
                    # Use KNN imputer for numeric data
                    n_neighbors = min(5, len(numeric_data) - 1) if len(numeric_data) > 1 else 1
                    if n_neighbors >= 1:
                        imputer = KNNImputer(n_neighbors=n_neighbors)
                        numeric_data_imputed = imputer.fit_transform(numeric_data)
                        df[numeric_cols] = numeric_data_imputed
                        logging.info(f"Imputed {numeric_cols} using KNN")

            # Handle categorical columns with mode imputation
            if categorical_cols:
                for col in categorical_cols:
                    if df[col].isnull().any():
                        # Use most frequent value
                        mode_value = df[col].mode()
                        if len(mode_value) > 0:
                            df[col].fillna(mode_value[0], inplace=True)
                        else:
                            df[col].fillna('Unknown', inplace=True)
                        logging.info(f"Imputed {col} using mode")

            # Convert back to list of dicts
            return df.to_dict('records')

        except Exception as e:
            logging.error(f"ML imputation failed: {e}, falling back to simple imputation")
            return self._simple_imputation(data, schema)

    def _simple_imputation(self, data, schema):
        """Simple rule-based imputation as fallback"""
        for record in data:
            for field, field_info in schema.items():
                if field not in record or record[field] is None or record[field] == '':
                    field_type = field_info.get('type', 'string')
                    semantic_cat = field_info.get('semantic_category', 'unknown')

                    # Smart defaults based on semantic category
                    if semantic_cat == 'monetary':
                        record[field] = 0.0
                    elif semantic_cat == 'quantity':
                        record[field] = 0
                    elif semantic_cat == 'rating':
                        record[field] = 0.0
                    elif semantic_cat == 'status':
                        record[field] = 'Unknown'
                    elif field_type in ['integer', 'float']:
                        record[field] = 0
                    else:
                        record[field] = None

        return data


class AnomalyDetector:
    """Detect anomalies and outliers in data"""

    def __init__(self):
        self.detector = None

    def detect_anomalies(self, data, schema):
        """
        Detect anomalous records using Isolation Forest
        Returns list of indices of anomalous records and anomaly scores
        """
        if not ML_AVAILABLE or len(data) < 10:
            return [], []

        try:
            df = pd.DataFrame(data)

            # Extract numeric features only
            numeric_cols = []
            for field, field_info in schema.items():
                if field in df.columns and field_info.get('type') in ['integer', 'float']:
                    numeric_cols.append(field)

            if not numeric_cols:
                return [], []

            # Prepare numeric data
            numeric_data = df[numeric_cols].copy()
            for col in numeric_cols:
                numeric_data[col] = pd.to_numeric(numeric_data[col], errors='coerce')

            # Fill NaN with median for anomaly detection
            numeric_data = numeric_data.fillna(numeric_data.median())

            # Detect anomalies
            iso_forest = IsolationForest(contamination=0.1, random_state=42)
            predictions = iso_forest.fit_predict(numeric_data)
            anomaly_scores = iso_forest.score_samples(numeric_data)

            # Get indices of anomalies (-1 means anomaly)
            anomaly_indices = [i for i, pred in enumerate(predictions) if pred == -1]

            logging.info(f"Detected {len(anomaly_indices)} anomalies out of {len(data)} records")

            return anomaly_indices, anomaly_scores.tolist()

        except Exception as e:
            logging.error(f"Anomaly detection failed: {e}")
            return [], []


class DataEnricher:
    """Enrich and validate data"""

    def __init__(self):
        pass

    def enrich_data(self, data, schema):
        """
        Enrich data with additional validated information
        """
        enriched_data = []

        for record in data:
            enriched_record = record.copy()

            # Add metadata
            enriched_record['_enrichment_timestamp'] = datetime.now().isoformat()
            enriched_record['_data_quality_score'] = self._calculate_quality_score(record, schema)

            # Validate and enrich specific fields
            for field, value in record.items():
                if field not in schema:
                    continue

                field_info = schema[field]
                semantic_cat = field_info.get('semantic_category', 'unknown')

                # Email validation and enrichment
                if semantic_cat == 'contact' or field_info.get('type') == 'email':
                    enriched_record[field] = self._validate_email(value)
                    if enriched_record[field]:
                        enriched_record[f'{field}_domain'] = enriched_record[field].split('@')[-1]

                # Phone number validation
                elif 'phone' in field.lower() or 'mobile' in field.lower():
                    enriched_record[field] = self._validate_phone(value)

                # URL validation
                elif semantic_cat == 'url' or field_info.get('type') == 'url':
                    enriched_record[f'{field}_is_valid'] = self._validate_url(value)

                # Monetary field enrichment
                elif semantic_cat == 'monetary':
                    enriched_record[f'{field}_formatted'] = self._format_money(value)

            enriched_data.append(enriched_record)

        return enriched_data

    def _calculate_quality_score(self, record, schema):
        """Calculate data quality score (0-100)"""
        total_fields = len(schema)
        filled_fields = 0
        valid_fields = 0

        for field in schema.keys():
            if field in record and record[field] is not None and record[field] != '':
                filled_fields += 1

                # Check if value seems valid (not placeholder)
                value = str(record[field]).lower()
                if value not in ['null', 'none', 'n/a', 'unknown', '']:
                    valid_fields += 1

        completeness_score = (filled_fields / total_fields) * 50 if total_fields > 0 else 0
        validity_score = (valid_fields / total_fields) * 50 if total_fields > 0 else 0

        return round(completeness_score + validity_score, 2)

    def _validate_email(self, email):
        """Validate and normalize email"""
        if not email or email == '':
            return None

        try:
            if ML_AVAILABLE:
                valid = validate_email(email)
                return valid.email
            else:
                # Simple regex validation
                pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                if re.match(pattern, str(email)):
                    return str(email).lower()
                return None
        except:
            return None

    def _validate_phone(self, phone):
        """Validate and format phone number"""
        if not phone or phone == '':
            return None

        try:
            if ML_AVAILABLE:
                parsed = phonenumbers.parse(str(phone), "US")
                if phonenumbers.is_valid_number(parsed):
                    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
            return str(phone)
        except:
            return str(phone)

    def _validate_url(self, url):
        """Check if URL is valid"""
        if not url:
            return False

        url_pattern = r'^https?://[^\s]+$'
        return bool(re.match(url_pattern, str(url)))

    def _format_money(self, value):
        """Format monetary value"""
        try:
            amount = float(value)
            return f"${amount:,.2f}"
        except:
            return str(value)


class DataDeduplicator:
    """Advanced deduplication using fuzzy matching"""

    def __init__(self):
        pass

    def deduplicate_advanced(self, data, identifier_fields=None):
        """
        Advanced deduplication using field similarity
        """
        if not data:
            return data, 0

        if identifier_fields is None:
            identifier_fields = ['id', 'email', 'name', 'user']

        unique_records = []
        seen_hashes = set()
        duplicate_count = 0

        for record in data:
            # Create a hash from identifier fields
            hash_parts = []
            for field in identifier_fields:
                if field in record and record[field]:
                    hash_parts.append(f"{field}:{str(record[field]).lower().strip()}")

            if hash_parts:
                record_hash = "|".join(sorted(hash_parts))

                if record_hash not in seen_hashes:
                    unique_records.append(record)
                    seen_hashes.add(record_hash)
                else:
                    duplicate_count += 1
            else:
                # No identifier found, keep record
                unique_records.append(record)

        logging.info(f"Advanced deduplication: {duplicate_count} duplicates removed")
        return unique_records, duplicate_count


# Global instances
missing_value_predictor = MissingValuePredictor()
anomaly_detector = AnomalyDetector()
data_enricher = DataEnricher()
data_deduplicator = DataDeduplicator()


def process_data_with_ml(data, schema):
    """
    Main function to process data with all ML enhancements
    """
    # 1. Deduplicate
    data, dup_count = data_deduplicator.deduplicate_advanced(data)

    # 2. Predict missing values
    data = missing_value_predictor.predict_missing_values(data, schema)

    # 3. Enrich data
    data = data_enricher.enrich_data(data, schema)

    # 4. Detect anomalies
    anomaly_indices, anomaly_scores = anomaly_detector.detect_anomalies(data, schema)

    # Add anomaly flag to data
    for i, record in enumerate(data):
        record['_is_anomaly'] = i in anomaly_indices
        if i < len(anomaly_scores):
            record['_anomaly_score'] = round(anomaly_scores[i], 3)

    return data, dup_count, len(anomaly_indices)
