"""Authentication module for AWARE Webservice Receiver"""

from flask import request, jsonify
from datetime import datetime, timedelta
import jwt
import logging
import os
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

TOKEN_SECRET = os.getenv('TOKEN_SECRET', 'your-secret-key-change-in-production')
TOKEN_EXPIRY_HOURS = int(os.getenv('TOKEN_EXPIRY_HOURS', 24))
STUDY_PASSWORD = os.getenv('STUDY_PASSWORD', 'aware_study_password')


def check_token():
    """Validate JWT token from Authorization header. Returns error tuple if invalid, None if valid."""
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'error': 'missing token'}), 401
    
    try:
        token = token.replace('Bearer ', '')
        jwt.decode(token, TOKEN_SECRET, algorithms=['HS256'])
    except jwt.InvalidTokenError:
        return jsonify({'error': 'invalid token'}), 401
    
    return None


def login(stats):
    """Authenticate and receive JWT token"""
    data = request.get_json()
    
    if not data or 'password' not in data:
        return jsonify({'error': 'missing password'}), 400
    
    if data['password'] != STUDY_PASSWORD:
        logger.warning("Unauthorized login attempt")
        stats['unauthorized_attempts'] += 1
        return jsonify({'error': 'invalid credentials'}), 401
    
    token = jwt.encode(
        {'exp': datetime.utcnow() + timedelta(hours=TOKEN_EXPIRY_HOURS)},
        TOKEN_SECRET,
        algorithm='HS256'
    )
    
    logger.info("Successful login")
    return jsonify({'token': token, 'expires_in': TOKEN_EXPIRY_HOURS * 3600}), 200
