"""
Analytics Engine with Trend Analysis and Change Detection
Provides insights, trends, and recommendations
"""

import logging
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import numpy as np

try:
    import pandas as pd
    from sklearn.linear_model import LinearRegression
    ANALYTICS_AVAILABLE = True
except ImportError:
    ANALYTICS_AVAILABLE = False
    logging.warning("Analytics dependencies not available")

from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION, MONGO_CHANGES_COLLECTION
from pymongo import MongoClient


class TrendAnalyzer:
    """Analyze trends in data over time"""

    def __init__(self):
        self.db = MongoClient(MONGO_URI)[MONGO_DB]
        self.collection = self.db[MONGO_COLLECTION]

    def analyze_field_trends(self, field_name, days=30):
        """
        Analyze trend for a specific field over time
        """
        if not ANALYTICS_AVAILABLE:
            return None

        try:
            # Get data from last N days
            cutoff_date = datetime.now() - timedelta(days=days)

            cursor = self.collection.find({
                '_loaded_at': {'$gte': cutoff_date},
                field_name: {'$exists': True, '$ne': None}
            }).sort('_loaded_at', 1)

            data = list(cursor)

            if len(data) < 2:
                return {'status': 'insufficient_data', 'message': 'Need at least 2 data points'}

            # Create time series
            timestamps = [record['_loaded_at'] for record in data]
            values = []

            for record in data:
                try:
                    val = float(record[field_name])
                    values.append(val)
                except (ValueError, TypeError):
                    continue

            if len(values) < 2:
                return {'status': 'non_numeric', 'message': 'Field is not numeric'}

            # Convert timestamps to numeric (days since first data point)
            first_timestamp = timestamps[0]
            numeric_times = [(ts - first_timestamp).total_seconds() / 86400 for ts in timestamps[:len(values)]]

            # Perform linear regression
            X = np.array(numeric_times).reshape(-1, 1)
            y = np.array(values)

            model = LinearRegression()
            model.fit(X, y)

            slope = model.coef_[0]
            r_squared = model.score(X, y)

            # Determine trend
            if abs(slope) < 0.01:
                trend = 'stable'
            elif slope > 0:
                trend = 'increasing'
            else:
                trend = 'decreasing'

            # Calculate statistics
            mean_value = np.mean(values)
            std_value = np.std(values)
            min_value = np.min(values)
            max_value = np.max(values)

            # Predict next value (1 day ahead)
            next_time = numeric_times[-1] + 1
            predicted_value = model.predict([[next_time]])[0]

            return {
                'field': field_name,
                'trend': trend,
                'slope': float(slope),
                'r_squared': float(r_squared),
                'statistics': {
                    'mean': float(mean_value),
                    'std': float(std_value),
                    'min': float(min_value),
                    'max': float(max_value),
                    'current': float(values[-1]),
                    'predicted_next': float(predicted_value)
                },
                'data_points': len(values),
                'period_days': days
            }

        except Exception as e:
            logging.error(f"Trend analysis failed: {e}")
            return {'status': 'error', 'message': str(e)}

    def get_all_trends(self, schema):
        """Get trends for all numeric fields"""
        trends = {}

        for field, field_info in schema.items():
            if field_info.get('type') in ['integer', 'float']:
                trend = self.analyze_field_trends(field)
                if trend and trend.get('status') != 'error':
                    trends[field] = trend

        return trends


class ChangeAnalyzer:
    """Advanced change detection and analysis"""

    def __init__(self):
        self.db = MongoClient(MONGO_URI)[MONGO_DB]
        self.changes_collection = self.db[MONGO_CHANGES_COLLECTION]

    def get_recent_changes(self, days=7, limit=100):
        """Get recent changes"""
        cutoff_date = datetime.now() - timedelta(days=days)

        changes = list(self.changes_collection.find({
            'timestamp': {'$gte': cutoff_date}
        }).sort('timestamp', -1).limit(limit))

        return changes

    def analyze_change_patterns(self, days=30):
        """Analyze patterns in changes"""
        cutoff_date = datetime.now() - timedelta(days=days)

        changes = list(self.changes_collection.find({
            'timestamp': {'$gte': cutoff_date}
        }))

        if not changes:
            return {'status': 'no_changes', 'message': 'No changes detected in period'}

        # Analyze by field
        changes_by_field = defaultdict(list)
        for change in changes:
            field = change.get('field')
            changes_by_field[field].append(change)

        analysis = {
            'total_changes': len(changes),
            'fields_changed': len(changes_by_field),
            'by_field': {}
        }

        for field, field_changes in changes_by_field.items():
            # Calculate change frequency
            change_values = []
            for ch in field_changes:
                old_val = ch.get('old_value')
                new_val = ch.get('new_value')

                try:
                    old_val_float = float(old_val)
                    new_val_float = float(new_val)
                    change_amount = new_val_float - old_val_float
                    change_percent = (change_amount / old_val_float * 100) if old_val_float != 0 else 0
                    change_values.append({
                        'amount': change_amount,
                        'percent': change_percent
                    })
                except (ValueError, TypeError):
                    pass

            field_analysis = {
                'change_count': len(field_changes),
                'frequency': len(field_changes) / days  # changes per day
            }

            if change_values:
                amounts = [cv['amount'] for cv in change_values]
                percents = [cv['percent'] for cv in change_values]

                field_analysis['average_change'] = {
                    'amount': float(np.mean(amounts)),
                    'percent': float(np.mean(percents))
                }
                field_analysis['volatility'] = float(np.std(amounts))

            analysis['by_field'][field] = field_analysis

        return analysis

    def detect_anomalous_changes(self, threshold_percent=50):
        """Detect unusually large changes"""
        cutoff_date = datetime.now() - timedelta(days=7)

        changes = list(self.changes_collection.find({
            'timestamp': {'$gte': cutoff_date}
        }))

        anomalous_changes = []

        for change in changes:
            old_val = change.get('old_value')
            new_val = change.get('new_value')

            try:
                old_val_float = float(old_val)
                new_val_float = float(new_val)

                if old_val_float != 0:
                    change_percent = abs((new_val_float - old_val_float) / old_val_float * 100)

                    if change_percent > threshold_percent:
                        anomalous_changes.append({
                            **change,
                            'change_percent': change_percent,
                            'severity': 'high' if change_percent > 100 else 'medium'
                        })
            except (ValueError, TypeError):
                continue

        return anomalous_changes


class RecommendationEngine:
    """Generate recommendations based on data analysis"""

    def __init__(self):
        self.trend_analyzer = TrendAnalyzer()
        self.change_analyzer = ChangeAnalyzer()

    def generate_recommendations(self, schema, stats):
        """Generate actionable recommendations"""
        recommendations = []

        # Data quality recommendations
        if stats.get('total_fields', 0) > 0:
            quality_score = self._calculate_overall_quality(stats)

            if quality_score < 70:
                recommendations.append({
                    'type': 'data_quality',
                    'priority': 'high',
                    'message': f'Overall data quality score is {quality_score}%. Consider improving data validation.',
                    'action': 'Review and enhance data validation rules'
                })

        # Schema recommendations
        for field, field_info in schema.items():
            # Check for low quality fields
            semantic_cat = field_info.get('semantic_category', 'unknown')

            if semantic_cat == 'unknown':
                recommendations.append({
                    'type': 'schema',
                    'priority': 'medium',
                    'message': f'Field "{field}" has unknown semantic category',
                    'action': 'Review field naming or add metadata'
                })

        # Trend-based recommendations
        trends = self.trend_analyzer.get_all_trends(schema)

        for field, trend_data in trends.items():
            if trend_data.get('trend') == 'decreasing':
                slope = trend_data.get('slope', 0)
                if slope < -1:  # Rapid decrease
                    recommendations.append({
                        'type': 'trend_alert',
                        'priority': 'high',
                        'message': f'Field "{field}" is rapidly decreasing (slope: {slope:.2f})',
                        'action': 'Investigate cause of decline'
                    })

        # Change pattern recommendations
        change_patterns = self.change_analyzer.analyze_change_patterns(days=7)

        if change_patterns.get('status') != 'no_changes':
            for field, field_changes in change_patterns.get('by_field', {}).items():
                frequency = field_changes.get('frequency', 0)

                if frequency > 5:  # More than 5 changes per day
                    recommendations.append({
                        'type': 'high_volatility',
                        'priority': 'medium',
                        'message': f'Field "{field}" changes {frequency:.1f} times per day',
                        'action': 'Consider if this volatility is expected'
                    })

        return recommendations

    def _calculate_overall_quality(self, stats):
        """Calculate overall data quality score"""
        # Simple heuristic based on stats
        score = 100

        # Penalize for missing fields
        if stats.get('duplicates_removed', 0) > stats.get('total_records', 1) * 0.1:
            score -= 20

        return max(0, score)


class DataInsights:
    """Generate insights from data"""

    def __init__(self):
        self.db = MongoClient(MONGO_URI)[MONGO_DB]
        self.collection = self.db[MONGO_COLLECTION]

    def get_field_distribution(self, field_name, limit=20):
        """Get distribution of values for a field"""
        pipeline = [
            {'$group': {'_id': f'${field_name}', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$limit': limit}
        ]

        results = list(self.collection.aggregate(pipeline))

        distribution = {
            'field': field_name,
            'values': [
                {'value': r['_id'], 'count': r['count']}
                for r in results
            ],
            'total_unique': len(results)
        }

        return distribution

    def get_summary_statistics(self, schema):
        """Get summary statistics for all fields"""
        total_records = self.collection.count_documents({})

        summary = {
            'total_records': total_records,
            'fields': {}
        }

        for field, field_info in schema.items():
            field_type = field_info.get('type', 'unknown')

            if field_type in ['integer', 'float']:
                # Numeric statistics
                pipeline = [
                    {'$group': {
                        '_id': None,
                        'avg': {'$avg': f'${field}'},
                        'min': {'$min': f'${field}'},
                        'max': {'$max': f'${field}'},
                        'count': {'$sum': 1}
                    }}
                ]

                result = list(self.collection.aggregate(pipeline))

                if result:
                    summary['fields'][field] = {
                        'type': field_type,
                        'statistics': result[0]
                    }
            else:
                # Categorical statistics
                distinct_count = len(self.collection.distinct(field))

                summary['fields'][field] = {
                    'type': field_type,
                    'unique_values': distinct_count
                }

        return summary


# Global instances
trend_analyzer = TrendAnalyzer()
change_analyzer = ChangeAnalyzer()
recommendation_engine = RecommendationEngine()
data_insights = DataInsights()
