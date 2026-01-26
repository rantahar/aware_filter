#!/usr/bin/env python3
"""
AWARE Webservice Receiver
Receives JSON POST data from AWARE clients and writes to MySQL database.
No filtering yet - just direct passthrough.
"""

from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error
import logging
import json
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

stats = {
    'total_requests': 0,
    'successful_inserts': 0,
    'failed_inserts': 0,
    'unauthorized_attempts': 0
}

DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DATABASE', 'aware_database'),
}

STUDY_PASSWORD = os.getenv('STUDY_PASSWORD', 'aware_study_password')
TABLE_NAME = os.getenv('TABLE_NAME', 'aware_data')

def get_db_connection():
    """Establish a database connection."""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def insert_data(data):
    """ Insert data into the database. """
    conn = get_db_connection()
    if conn is None:
        return False, "Database connection failed"
    
    try:
        cursor = conn.cursor()

        # Build INSERT query
        columns = ', '.join(f'`{key}`' for key in data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO `{TABLE_NAME}` ({columns}) VALUES ({placeholders})"

        cursor.execute(query, list(data.values()))
        conn.commit()

        logger.info("Data inserted successfully")
        return True, "Data inserted successfully"

    except Error as e:
        logger.error(f"Error inserting data: {e}")
        return False, str(e)


@app.route('/webservice/index/<study_id>/<password>', methods=['POST'])
def webservice_root(study_id, password):
    """Root endpoint - AWARE may POST here with table name in data"""
    if password != STUDY_PASSWORD:
        logger.warning(f"Unauthorized attempt: study_id={study_id}")
        stats['unauthorized_attempts'] += 1
        return jsonify({'error': 'unauthorized'}), 401
    
    stats['total_requests'] += 1
    
    try:
        data = request.get_json()
        
        # Log raw data to understand format
        logger.info(f"POST to root endpoint")
        logger.info(f"Content-Type: {request.content_type}")
        logger.info(f"Data keys: {data.keys() if isinstance(data, dict) else 'list'}")
        logger.info(f"Full data: {json.dumps(data, default=str)[:1000]}")


@app.route('/webservice/index/<study_id>/<password>/<table_name>', methods=['POST'])
def webservice_table(study_id, password, table_name):
    """
    Table-specific endpoint - receives data for specific table
    """
    if password != STUDY_PASSWORD:
        logger.warning(f"Unauthorized attempt: study_id={study_id}, table={table_name}")
        return jsonify({'error': 'unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'no data'}), 400
        
        # Handle both single object and array of objects
        if isinstance(data, list):
            logger.info(f"Received {len(data)} records for table: {table_name}")
            success_count = 0
            error_count = 0
            
            for record in data:
                success, msg = insert_data(table_name, record)
                if success:
                    success_count += 1
                else:
                    error_count += 1
                    logger.error(f"Failed to insert record: {msg}")
            
            return jsonify({
                'status': 'ok',
                'inserted': success_count,
                'errors': error_count
            }), 200
            
        else:
            # Single record
            logger.info(f"Received 1 record for table: {table_name}")
            success, msg = insert_data(table_name, data)
            
            if success:
                return jsonify({'status': 'ok'}), 200
            else:
                return jsonify({'error': msg}), 500
        
    except Exception as e:
        logger.error(f"Error processing request: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    conn = get_db_connection()
    if conn:
        conn.close()
        return jsonify({'status': 'healthy', 'database': 'connected'}), 200
    else:
        return jsonify({'status': 'unhealthy', 'database': 'disconnected'}), 503


@app.route('/stats', methods=['GET'])
def stats():
    """Basic stats endpoint for monitoring"""
    return jsonify({
        'service': 'AWARE Webservice Receiver',
        'timestamp': datetime.utcnow().isoformat(),
        'endpoints': [
            '/webservice/index/<study_id>/<password>',
            '/webservice/index/<study_id>/<password>/<table_name>'
        ]
    }), 200

if __name__ == '__main__':
    logger.info("Starting AWARE Webservice Receiver")
    logger.info(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    # Run with HTTPS (self-signed cert for testing)
    app.run(
        host='0.0.0.0',
        port=8443,
        ssl_context='adhoc',  # Self-signed cert for testing
        debug=False
    )

