"""
Simplified REST API Endpoints (Working Version)
"""

from flask import Blueprint, request, jsonify
import logging
from datetime import datetime

# Create Blueprint
api = Blueprint('api', __name__, url_prefix='/api/v1')


@api.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0',
        'message': 'AI-Powered Pipeline is running!'
    })


@api.route('/ping', methods=['GET'])
def ping():
    """Simple ping endpoint"""
    return jsonify({'ping': 'pong'})


@api.route('/info', methods=['GET'])
def info():
    """System information"""
    return jsonify({
        'app_name': 'AI-Powered Data Pipeline',
        'features': [
            'Multi-format parsing (JSON, CSV, HTML, PDF)',
            'AI-powered schema detection',
            'ML-based data quality',
            'Multi-database support',
            'Real-time analytics'
        ],
        'endpoints': {
            'health': '/api/v1/health',
            'info': '/api/v1/info',
            'upload': '/ (POST with file)',
            'dashboard': 'http://localhost:8050'
        }
    })
