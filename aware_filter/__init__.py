#!/usr/bin/env python3
"""
AWARE Webservice Receiver
Receives JSON POST data from AWARE clients and writes to MySQL database.
No filtering yet - just direct passthrough.
"""

from flask import Flask, jsonify, request
import logging
import psutil
import gc
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

def check_memory_usage():
    """Check current memory usage and log warnings if high"""
    process = psutil.Process()
    memory_info = process.memory_info()
    memory_mb = memory_info.rss / 1024 / 1024
    
    if memory_mb > 400:  # Warning if over 400MB
        logger.warning(f"High memory usage: {memory_mb:.1f} MB")
        if memory_mb > 500:  # Force garbage collection if over 500MB
            gc.collect()
            logger.info("Forced garbage collection due to high memory usage")
    
    return memory_mb

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
    try:
        # Check memory usage before processing request
        memory_mb = check_memory_usage()
        logger.debug(f"Processing data query request. Current memory: {memory_mb:.1f} MB")
        
        # Validate token
        token_error = check_token()
        if token_error:
            return token_error
        
        table_name = request.args.get('table')
        if not table_name:
            return jsonify({'error': 'missing table parameter'}), 400
        
        # Build WHERE conditions from query parameters
        conditions = []
        params = []
        limit = None
        offset = None
        
        for key, value in request.args.items():
            if key == 'table':  # Skip the table parameter
                continue
            elif key == 'start_time':
                conditions.append('`timestamp` >= %s')
                params.append(value)
            elif key == 'end_time':
                conditions.append('`timestamp` <= %s')
                params.append(value)
            elif key == 'limit':
                try:
                    limit = int(value)
                    if limit <= 0:
                        return jsonify({'error': 'limit must be positive'}), 400
                except ValueError:
                    return jsonify({'error': 'limit must be a valid integer'}), 400
            elif key == 'offset':
                try:
                    offset = int(value)
                    if offset < 0:
                        return jsonify({'error': 'offset must be non-negative'}), 400
                except ValueError:
                    return jsonify({'error': 'offset must be a valid integer'}), 400
            else:
                conditions.append(f'`{key}` = %s')
                params.append(value)
        
        success, response_dict, status_code = query_table(table_name, conditions, params, limit, offset)
        
        # Check memory usage after query but before JSON serialization
        memory_after_query = check_memory_usage()
        logger.debug(f"After database query. Memory: {memory_after_query:.1f} MB")
        
        # Add a warning to response if dataset is large
        if 'total_count' in response_dict and response_dict['total_count'] > 100000:
            response_dict['warning'] = f"Large dataset ({response_dict['total_count']} total records). Consider using pagination with limit and offset parameters."
        
        return jsonify(response_dict), status_code
    
    except Exception as e:
        logger.error(f"Unexpected error in query route: {e}")
        # Force garbage collection on error
        gc.collect()
        return jsonify({'error': 'Internal server error'}), 500

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

