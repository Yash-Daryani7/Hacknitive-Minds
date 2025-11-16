"""
AI-Powered Schema Inference Module
Uses NLP, transformers, and ML for intelligent schema detection and field classification
"""

import re
import logging
from collections import defaultdict, Counter
from datetime import datetime
import numpy as np

# Will be imported after installation
try:
    from sentence_transformers import SentenceTransformer
    from sklearn.cluster import DBSCAN
    import pandas as pd
    DEPENDENCIES_AVAILABLE = True
except ImportError:
    DEPENDENCIES_AVAILABLE = False
    logging.warning("AI dependencies not installed. Install requirements.txt")


class SemanticFieldClassifier:
    """Classifies fields based on semantic meaning using NLP"""

    # Field semantic categories with keywords (Expanded with 30+ categories)
    FIELD_CATEGORIES = {
        # Financial & Monetary
        'monetary': {
            'keywords': ['price', 'cost', 'amount', 'salary', 'fee', 'rate', 'charge', 'total', 'subtotal', 'tax', 'discount', 'revenue', 'budget', 'payment', 'invoice', 'bill'],
            'patterns': [r'\$', r'price', r'cost', r'amount', r'usd', r'eur', r'payment'],
            'semantic_type': 'financial',
            'icon': '💰'
        },
        'financial_account': {
            'keywords': ['account', 'iban', 'swift', 'routing', 'bank', 'credit', 'debit'],
            'patterns': [r'account', r'iban', r'swift', r'bank'],
            'semantic_type': 'financial',
            'icon': '🏦'
        },

        # Identity & Authentication
        'identifier': {
            'keywords': ['id', 'identifier', 'uuid', 'key', 'code', 'reference', 'number', 'serial', 'sku'],
            'patterns': [r'^id$', r'_id$', r'uuid', r'code', r'sku'],
            'semantic_type': 'identity',
            'icon': '🔑'
        },
        'authentication': {
            'keywords': ['password', 'token', 'secret', 'hash', 'salt', 'api_key', 'auth'],
            'patterns': [r'password', r'token', r'secret', r'auth', r'key'],
            'semantic_type': 'security',
            'icon': '🔐'
        },
        'session': {
            'keywords': ['session', 'cookie', 'jwt', 'bearer', 'oauth'],
            'patterns': [r'session', r'cookie', r'jwt', r'oauth'],
            'semantic_type': 'security',
            'icon': '🎫'
        },

        # Personal Information
        'personal_name': {
            'keywords': ['name', 'firstname', 'lastname', 'fullname', 'username', 'author', 'person', 'display_name'],
            'patterns': [r'name', r'user', r'author'],
            'semantic_type': 'personal_info',
            'icon': '👤'
        },
        'age_demographic': {
            'keywords': ['age', 'birthdate', 'dob', 'birthday', 'born'],
            'patterns': [r'age', r'birth', r'dob'],
            'semantic_type': 'personal_info',
            'icon': '🎂'
        },
        'gender': {
            'keywords': ['gender', 'sex', 'pronoun'],
            'patterns': [r'gender', r'sex'],
            'semantic_type': 'personal_info',
            'icon': '⚥'
        },

        # Contact Information
        'contact': {
            'keywords': ['email', 'phone', 'mobile', 'tel', 'contact', 'fax', 'cell'],
            'patterns': [r'email', r'phone', r'mobile', r'contact', r'@'],
            'semantic_type': 'contact_info',
            'icon': '📧'
        },
        'social_media': {
            'keywords': ['twitter', 'facebook', 'linkedin', 'instagram', 'social', 'handle'],
            'patterns': [r'twitter', r'facebook', r'social', r'@'],
            'semantic_type': 'contact_info',
            'icon': '📱'
        },

        # Temporal
        'temporal': {
            'keywords': ['date', 'time', 'timestamp', 'created', 'updated', 'modified', 'year', 'month', 'day', 'scheduled'],
            'patterns': [r'date', r'time', r'timestamp', r'_at$', r'_on$', r'when'],
            'semantic_type': 'temporal',
            'icon': '📅'
        },
        'duration': {
            'keywords': ['duration', 'period', 'interval', 'elapsed', 'length', 'timeout'],
            'patterns': [r'duration', r'period', r'interval', r'elapsed'],
            'semantic_type': 'temporal',
            'icon': '⏱️'
        },

        # Geographic & Location
        'location': {
            'keywords': ['address', 'city', 'state', 'country', 'zip', 'postal', 'location', 'region', 'province'],
            'patterns': [r'address', r'city', r'country', r'location', r'zip'],
            'semantic_type': 'geographic',
            'icon': '📍'
        },
        'coordinates': {
            'keywords': ['latitude', 'longitude', 'lat', 'lng', 'lon', 'coord', 'geo'],
            'patterns': [r'lat', r'lng', r'lon', r'coord', r'geo'],
            'semantic_type': 'geographic',
            'icon': '🌍'
        },

        # Measurement & Metrics
        'rating': {
            'keywords': ['rating', 'score', 'rank', 'stars', 'review', 'feedback', 'grade'],
            'patterns': [r'rating', r'score', r'rank', r'stars'],
            'semantic_type': 'measurement',
            'icon': '⭐'
        },
        'quantity': {
            'keywords': ['quantity', 'count', 'number', 'total', 'amount', 'volume', 'size', 'stock', 'inventory'],
            'patterns': [r'qty', r'quantity', r'count', r'num', r'stock'],
            'semantic_type': 'measurement',
            'icon': '📊'
        },
        'percentage': {
            'keywords': ['percentage', 'percent', 'ratio', 'rate', 'proportion'],
            'patterns': [r'percent', r'%', r'ratio', r'rate'],
            'semantic_type': 'measurement',
            'icon': '📈'
        },
        'weight_mass': {
            'keywords': ['weight', 'mass', 'kg', 'lb', 'gram', 'ounce'],
            'patterns': [r'weight', r'mass', r'kg', r'lb', r'gram'],
            'semantic_type': 'measurement',
            'icon': '⚖️'
        },
        'temperature': {
            'keywords': ['temperature', 'temp', 'celsius', 'fahrenheit', 'kelvin'],
            'patterns': [r'temp', r'celsius', r'fahrenheit'],
            'semantic_type': 'measurement',
            'icon': '🌡️'
        },

        # Text & Content
        'description': {
            'keywords': ['description', 'details', 'info', 'summary', 'text', 'content', 'notes', 'bio', 'about'],
            'patterns': [r'desc', r'description', r'details', r'text', r'bio'],
            'semantic_type': 'textual',
            'icon': '📝'
        },
        'title_heading': {
            'keywords': ['title', 'heading', 'headline', 'subject', 'topic'],
            'patterns': [r'title', r'heading', r'headline', r'subject'],
            'semantic_type': 'textual',
            'icon': '📰'
        },
        'comment_message': {
            'keywords': ['comment', 'message', 'post', 'reply', 'chat', 'note'],
            'patterns': [r'comment', r'message', r'post', r'chat'],
            'semantic_type': 'textual',
            'icon': '💬'
        },

        # Media & Files
        'url': {
            'keywords': ['url', 'link', 'website', 'uri', 'href', 'endpoint'],
            'patterns': [r'url', r'link', r'website', r'http', r'href'],
            'semantic_type': 'reference',
            'icon': '🔗'
        },
        'file_path': {
            'keywords': ['file', 'path', 'filename', 'directory', 'folder', 'attachment'],
            'patterns': [r'file', r'path', r'filename', r'\.'],
            'semantic_type': 'reference',
            'icon': '📁'
        },
        'image_media': {
            'keywords': ['image', 'photo', 'picture', 'avatar', 'thumbnail', 'icon', 'logo'],
            'patterns': [r'image', r'photo', r'picture', r'avatar', r'img'],
            'semantic_type': 'media',
            'icon': '🖼️'
        },
        'video_audio': {
            'keywords': ['video', 'audio', 'media', 'mp4', 'mp3', 'stream'],
            'patterns': [r'video', r'audio', r'media', r'mp4', r'mp3'],
            'semantic_type': 'media',
            'icon': '🎬'
        },

        # Status & State
        'status': {
            'keywords': ['status', 'state', 'condition', 'flag', 'active', 'enabled', 'live', 'published'],
            'patterns': [r'status', r'state', r'is_', r'has_', r'enabled'],
            'semantic_type': 'categorical',
            'icon': '🚦'
        },
        'priority': {
            'keywords': ['priority', 'importance', 'urgency', 'severity', 'level'],
            'patterns': [r'priority', r'importance', r'urgency', r'severity'],
            'semantic_type': 'categorical',
            'icon': '⚡'
        },

        # Classification
        'category': {
            'keywords': ['category', 'type', 'kind', 'class', 'group', 'tag', 'genre', 'department'],
            'patterns': [r'category', r'type', r'kind', r'class', r'dept'],
            'semantic_type': 'categorical',
            'icon': '🏷️'
        },
        'tag_label': {
            'keywords': ['tag', 'label', 'badge', 'keyword', 'hashtag'],
            'patterns': [r'tag', r'label', r'badge', r'#'],
            'semantic_type': 'categorical',
            'icon': '#️⃣'
        },

        # Health & Medical
        'medical': {
            'keywords': ['diagnosis', 'symptom', 'disease', 'condition', 'medication', 'prescription', 'patient'],
            'patterns': [r'medical', r'diagnosis', r'patient', r'symptom'],
            'semantic_type': 'medical',
            'icon': '🏥'
        },
        'health_metric': {
            'keywords': ['blood_pressure', 'heart_rate', 'glucose', 'bmi', 'pulse'],
            'patterns': [r'blood', r'heart', r'pulse', r'bmi'],
            'semantic_type': 'medical',
            'icon': '❤️'
        },

        # Education
        'academic': {
            'keywords': ['grade', 'course', 'subject', 'degree', 'major', 'gpa', 'student', 'teacher'],
            'patterns': [r'grade', r'course', r'degree', r'gpa', r'student'],
            'semantic_type': 'education',
            'icon': '🎓'
        },

        # Business & Commerce
        'product': {
            'keywords': ['product', 'item', 'goods', 'merchandise', 'sku', 'model'],
            'patterns': [r'product', r'item', r'sku', r'model'],
            'semantic_type': 'commerce',
            'icon': '📦'
        },
        'company': {
            'keywords': ['company', 'organization', 'business', 'enterprise', 'firm', 'corporation'],
            'patterns': [r'company', r'org', r'business', r'corp'],
            'semantic_type': 'business',
            'icon': '🏢'
        },

        # Technical & System
        'ip_network': {
            'keywords': ['ip', 'ipaddress', 'hostname', 'domain', 'server', 'port'],
            'patterns': [r'ip', r'host', r'server', r'port', r'\d+\.\d+\.\d+'],
            'semantic_type': 'technical',
            'icon': '🌐'
        },
        'version': {
            'keywords': ['version', 'release', 'build', 'revision'],
            'patterns': [r'version', r'v\d+', r'release', r'build'],
            'semantic_type': 'technical',
            'icon': '🔢'
        },
        'error_log': {
            'keywords': ['error', 'exception', 'warning', 'log', 'debug', 'trace'],
            'patterns': [r'error', r'exception', r'warning', r'log'],
            'semantic_type': 'technical',
            'icon': '⚠️'
        }
    }

    def __init__(self):
        self.model = None
        if DEPENDENCIES_AVAILABLE:
            try:
                # Use lightweight model for field name embeddings
                self.model = SentenceTransformer('all-MiniLM-L6-v2')
                logging.info("Semantic field classifier initialized with embeddings model")
            except Exception as e:
                logging.warning(f"Could not load transformer model: {e}")

    def classify_field_name(self, field_name):
        """Classify field based on its name using pattern matching and keywords"""
        field_lower = field_name.lower().strip()

        # Score each category
        scores = {}
        for category, info in self.FIELD_CATEGORIES.items():
            score = 0

            # Check keywords
            for keyword in info['keywords']:
                if keyword in field_lower:
                    score += 10

            # Check patterns
            for pattern in info['patterns']:
                if re.search(pattern, field_lower):
                    score += 5

            scores[category] = score

        # Return category with highest score
        if max(scores.values()) > 0:
            best_category = max(scores, key=scores.get)
            category_info = self.FIELD_CATEGORIES[best_category]
            return {
                'category': best_category,
                'semantic_type': category_info['semantic_type'],
                'icon': category_info.get('icon', '📊'),
                'confidence': min(scores[best_category] / 20.0, 1.0)  # Normalize to 0-1
            }

        return {
            'category': 'unknown',
            'semantic_type': 'generic',
            'icon': '❓',
            'confidence': 0.0
        }

    def classify_field_values(self, values):
        """Classify field based on its values using statistical analysis"""
        if not values:
            return {'value_pattern': 'empty'}

        # Remove None/empty values
        clean_values = [v for v in values if v is not None and str(v).strip()]

        if not clean_values:
            return {'value_pattern': 'all_null'}

        # Convert unhashable types (lists, dicts) to hashable types for set operations
        def make_hashable(value):
            """Convert value to hashable type"""
            if isinstance(value, list):
                return tuple(make_hashable(v) for v in value)
            elif isinstance(value, dict):
                return tuple(sorted((k, make_hashable(v)) for k, v in value.items()))
            else:
                return value

        # Analyze value patterns
        hashable_values = [make_hashable(v) for v in clean_values]

        analysis = {
            'unique_ratio': len(set(hashable_values)) / len(hashable_values),
            'avg_length': np.mean([len(str(v)) for v in clean_values]),
            'has_numbers': any(bool(re.search(r'\d', str(v))) for v in clean_values[:100]),
            'has_special_chars': any(bool(re.search(r'[^a-zA-Z0-9\s]', str(v))) for v in clean_values[:100])
        }

        # Determine pattern
        if analysis['unique_ratio'] > 0.95:
            analysis['value_pattern'] = 'high_cardinality'  # Likely IDs or unique identifiers
        elif analysis['unique_ratio'] < 0.1:
            analysis['value_pattern'] = 'low_cardinality'  # Likely categories or statuses
        else:
            analysis['value_pattern'] = 'medium_cardinality'

        return analysis

    def get_field_embedding(self, field_name):
        """Get semantic embedding for field name"""
        if self.model is None:
            return None

        try:
            embedding = self.model.encode([field_name])[0]
            return embedding
        except Exception as e:
            logging.warning(f"Could not generate embedding: {e}")
            return None


class AISchemaInference:
    """AI-powered schema inference with semantic understanding"""

    def __init__(self):
        self.classifier = SemanticFieldClassifier()
        self.schema_embeddings = {}

    def infer_enhanced_schema(self, batch, current_schema=None):
        """
        Infer schema with AI-powered semantic classification
        Returns enhanced schema with semantic metadata
        """
        if current_schema is None:
            current_schema = {}

        # Collect field values
        field_values = defaultdict(list)
        for record in batch:
            for key, value in record.items():
                field_values[key].append(value)

        # Analyze each field
        for field_name, values in field_values.items():
            # Get basic type from existing logic
            from transform import infer_field_type, detect_type
            inferred_type = infer_field_type(values)

            # Get semantic classification from field name
            semantic_info = self.classifier.classify_field_name(field_name)

            # Get value pattern analysis
            value_analysis = self.classifier.classify_field_values(values)

            # Get embedding
            embedding = self.classifier.get_field_embedding(field_name)

            # Build enhanced schema entry
            if field_name not in current_schema:
                current_schema[field_name] = {
                    'type': inferred_type,
                    'sample_values': values[:3],
                    'semantic_category': semantic_info['category'],
                    'semantic_type': semantic_info['semantic_type'],
                    'confidence': semantic_info['confidence'],
                    'value_analysis': value_analysis,
                    'first_seen': datetime.now().isoformat(),
                    'occurrence_count': len(values)
                }

                # Store embedding separately (not in MongoDB schema)
                if embedding is not None:
                    self.schema_embeddings[field_name] = embedding

                logging.info(f"New field detected: {field_name} -> {semantic_info['category']} ({inferred_type})")
            else:
                # Update existing field
                current_schema[field_name]['occurrence_count'] = current_schema[field_name].get('occurrence_count', 0) + len(values)
                current_schema[field_name]['last_seen'] = datetime.now().isoformat()

        return current_schema

    def find_similar_schemas(self, current_schema, schema_history, top_k=3):
        """
        Find similar schemas from history using embedding similarity
        """
        if not DEPENDENCIES_AVAILABLE or not self.schema_embeddings:
            return []

        if not schema_history:
            return []

        try:
            # Get current schema embedding (average of field embeddings)
            current_embeddings = [
                self.schema_embeddings.get(field, None)
                for field in current_schema.keys()
            ]
            current_embeddings = [e for e in current_embeddings if e is not None]

            if not current_embeddings:
                return []

            current_avg_embedding = np.mean(current_embeddings, axis=0)

            # Compare with historical schemas
            similarities = []
            for hist_schema in schema_history:
                hist_fields = hist_schema.get('schema', {}).keys()
                hist_embeddings = [
                    self.classifier.get_field_embedding(field)
                    for field in hist_fields
                ]
                hist_embeddings = [e for e in hist_embeddings if e is not None]

                if hist_embeddings:
                    hist_avg_embedding = np.mean(hist_embeddings, axis=0)

                    # Cosine similarity
                    similarity = np.dot(current_avg_embedding, hist_avg_embedding) / (
                        np.linalg.norm(current_avg_embedding) * np.linalg.norm(hist_avg_embedding)
                    )

                    similarities.append({
                        'schema_version': hist_schema.get('version'),
                        'similarity': float(similarity),
                        'schema': hist_schema
                    })

            # Sort by similarity and return top k
            similarities.sort(key=lambda x: x['similarity'], reverse=True)
            return similarities[:top_k]

        except Exception as e:
            logging.error(f"Error finding similar schemas: {e}")
            return []

    def suggest_field_mappings(self, source_field, target_schema):
        """
        Suggest which field in target schema maps to source field
        Based on semantic similarity
        """
        if not self.schema_embeddings:
            return None

        source_embedding = self.classifier.get_field_embedding(source_field)
        if source_embedding is None:
            return None

        best_match = None
        best_similarity = 0.0

        for target_field in target_schema.keys():
            target_embedding = self.schema_embeddings.get(target_field)
            if target_embedding is None:
                target_embedding = self.classifier.get_field_embedding(target_field)

            if target_embedding is not None:
                similarity = np.dot(source_embedding, target_embedding) / (
                    np.linalg.norm(source_embedding) * np.linalg.norm(target_embedding)
                )

                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match = target_field

        if best_similarity > 0.7:  # Threshold for good match
            return {
                'target_field': best_match,
                'similarity': float(best_similarity),
                'confidence': 'high' if best_similarity > 0.85 else 'medium'
            }

        return None


# Global instance
ai_schema_inferencer = AISchemaInference()


def infer_schema_with_ai(batch, current_schema=None):
    """
    Main function to infer schema with AI enhancements
    Drop-in replacement for the basic infer_schema function
    """
    return ai_schema_inferencer.infer_enhanced_schema(batch, current_schema)
