#!/usr/bin/env python3
"""
AWARE Webservice Receiver
Receives JSON POST data from AWARE clients and writes to MySQL database.
No filtering yet - just direct passthrough.
"""

from flask import Flask, jsonify, request
import logging
from datetime import datetime
from dotenv import load_dotenv
import os
from .auth import login, check_token
from .insertion import insert_records, get_db_connection, DB_CONFIG, STUDY_PASSWORD
from .retrieval import query_data, query_table

load_dotenv()

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

stats = {
    'total_requests': 0,
    'successful_inserts': 0,
    'failed_inserts': 0,
    'unauthorized_attempts': 0
}

@app.route('/webservice/index/<study_id>/<password>/<table_name>', methods=['POST'])
def webservice_table_route(study_id, password, table_name):
    """
    Table-specific endpoint - receives data for specific table
    """
    if password != STUDY_PASSWORD:
        logger.warning(f"Unauthorized attempt: study_id={study_id}, table={table_name}")
        stats['unauthorized_attempts'] += 1
        return jsonify({'error': 'unauthorized'}), 401
    
    stats['total_requests'] += 1
    
    try:
        data = request.get_json()
        success, response_dict = insert_records(data, table_name, stats)
        
        if success:
            return jsonify(response_dict), 200
        else:
            return jsonify(response_dict), 500
        
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
def get_stats():
    """Stats endpoint for monitoring"""
    return jsonify({
        'service': 'AWARE Webservice Receiver',
        'timestamp': datetime.utcnow().isoformat(),
        'stats': stats,
        'endpoints': [
            '/webservice/index/<study_id>/<password>',
            '/webservice/index/<study_id>/<password>/<table_name>'
        ]
    }), 200

@app.route('/login', methods=['POST'])
def login_route():
    """Authenticate and receive JWT token"""
    return login(stats)


@app.route('/data', methods=['GET'])
def query_route():
    """Generic query endpoint - query any table with any conditions"""
    # Validate token
    token_error = check_token()
    if token_error:
        return token_error
    
    table_name = request.args.get('table')
    if not table_name:
        return jsonify({'error': 'missing table parameter'}), 400
    
    # Build WHERE conditions from all other query parameters
    conditions = []
    params = []
    
    for key, value in request.args.items():
        if key == 'table':  # Skip the table parameter
            continue
        elif key == 'start_time':
            conditions.append('`timestamp` >= %s')
            params.append(value)
        elif key == 'end_time':
            conditions.append('`timestamp` <= %s')
            params.append(value)
        else:
            conditions.append(f'`{key}` = %s')
            params.append(value)
    
    success, response_dict, status_code = query_table(table_name, conditions, params)
    
    return jsonify(response_dict), status_code

def main():
    """Entry point for the aware-filter command"""
    logger.info("Starting AWARE Webservice Receiver")
    logger.info(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    app.run(
        host='0.0.0.0',
        port=3446,
        ssl_context='adhoc',
        debug=False
    )

if __name__ == '__main__':
    main()

