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
import time
from datetime import datetime
from dotenv import load_dotenv
import os
from .auth import login, check_token
from .insertion import insert_records, STUDY_PASSWORD
from .retrieval import query_table, get_all_tables
from .connection import get_connection

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
    
    Path Parameters:
        study_id (str): Study identifier
        password (str): Study password for authentication
        table_name (str): Name of the table to insert data into
    
    Request Body:
        JSON array or object containing records to insert
    
    Returns:
        200: Successful insertion with record count
        401: Invalid password
        500: Database error or insertion failure
    """
    route_start_time = time.time()
    if password != STUDY_PASSWORD:
        logger.warning(f"Unauthorized attempt: study_id={study_id}, table={table_name}")
        stats['unauthorized_attempts'] += 1
        return jsonify({'error': 'unauthorized'}), 401
    
    stats['total_requests'] += 1
    
    try:
        data = request.get_json()
        success, response_dict = insert_records(data, table_name, stats)
        
        if success:
            elapsed = time.time() - route_start_time
            logger.info(f"webservice_table_route completed in {elapsed:.3f}s")
            return jsonify(response_dict), 200
        else:
            elapsed = time.time() - route_start_time
            logger.info(f"webservice_table_route completed in {elapsed:.3f}s (failure)")
            return jsonify(response_dict), 500
        
    except Exception as e:
        elapsed = time.time() - route_start_time
        logger.error(f"Error processing request after {elapsed:.3f}s: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """
    Health check endpoint
    
    Returns:
        200: Service is healthy and database is connected
        503: Database connection failed or service is unhealthy
    """
    route_start_time = time.time()
    conn = get_connection()
    elapsed = time.time() - route_start_time
    if conn:
        logger.debug(f"health endpoint completed in {elapsed:.3f}s")
        return jsonify({'status': 'healthy', 'database': 'connected'}), 200
    else:
        logger.debug(f"health endpoint completed in {elapsed:.3f}s (unhealthy)")
        return jsonify({'status': 'unhealthy', 'database': 'disconnected'}), 503


@app.route('/stats', methods=['GET'])
def get_stats():
    """
    Stats endpoint for monitoring
    
    Returns:
        Service name, current timestamp, and statistics including:
        - total_requests: Total requests processed
        - successful_inserts: Successful data insertions
        - failed_inserts: Failed data insertions
        - unauthorized_attempts: Failed authentication attempts
    """
    route_start_time = time.time()
    result = jsonify({
        'service': 'AWARE Webservice Receiver',
        'timestamp': datetime.utcnow().isoformat(),
        'stats': stats,
        'endpoints': [
            '/webservice/index/<study_id>/<password>',
            '/webservice/index/<study_id>/<password>/<table_name>'
        ]
    })
    elapsed = time.time() - route_start_time
    logger.debug(f"stats endpoint completed in {elapsed:.3f}s")
    return result, 200

@app.route('/login', methods=['POST'])
def login_route():
    """
    Authenticate and receive JWT token
    
    Request Body:
        JSON object with authentication credentials
    
    Returns:
        200: Authentication successful, returns JWT token
        401: Authentication failed
    """
    route_start_time = time.time()
    result = login(stats)
    elapsed = time.time() - route_start_time
    logger.info(f"login endpoint completed in {elapsed:.3f}s")
    return result


@app.route('/data', methods=['GET'])
def query_route():
    """
    Generic query endpoint - query any table with any conditions
    
    Query Parameters:
        table (str, required): Name of the table to query
        device_id (str, optional): Filter by device_id column
        device_uid (str, optional): Filter by device_uid column
        timestamp (str, optional): Filter by exact timestamp
        start_time (str, optional): Filter records with timestamp >= start_time
        end_time (str, optional): Filter records with timestamp <= end_time
        limit (int, optional): Maximum records to return (default: 10000, max: 50000)
        offset (int, optional): Number of records to skip for pagination (default: 0)
        <any_column> (str, optional): Filter by any table column using equality
    
    Returns:
        200: Query successful with data, count, total_count, limit, offset, has_more
        400: Invalid parameters or missing table name
        404: Table not found
        500: Database error
    """
    request_start_time = datetime.utcnow()
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
        
        # Calculate request duration
        request_duration = (datetime.utcnow() - request_start_time).total_seconds()
        
        # Check memory usage after query but before JSON serialization
        memory_after_query = check_memory_usage()
        logger.debug(f"After database query. Memory: {memory_after_query:.1f} MB, Duration: {request_duration:.1f}s")
        
        # Add warnings to response
        warnings = []
        if 'total_count' in response_dict and response_dict['total_count'] > 100000:
            warnings.append(f"Large dataset ({response_dict['total_count']} total records). Consider using pagination with limit and offset parameters.")
        
        if request_duration > 60:  # Warn if query takes more than 1 minute
            warnings.append(f"Long-running query ({request_duration:.1f}s). Consider adding more specific filters or pagination.")
            logger.warning(f"Long query duration: {request_duration:.1f}s for table {table_name}")
        
        if warnings:
            response_dict['warnings'] = warnings
        
        response_dict['query_duration_seconds'] = round(request_duration, 2)
        
        return jsonify(response_dict), status_code
    
    except Exception as e:
        request_duration = (datetime.utcnow() - request_start_time).total_seconds()
        logger.error(f"Unexpected error in query route after {request_duration:.1f}s: {e}")
        
        # Check if this might be a timeout-related error
        if request_duration > 240:  # Close to our 300s timeout
            logger.error(f"Query likely timed out after {request_duration:.1f}s. Consider using pagination or more specific filters.")
            return jsonify({
                'error': 'Query timeout - request took too long to process',
                'suggestion': 'Use limit/offset parameters or more specific filters to reduce dataset size',
                'duration_seconds': round(request_duration, 2)
            }), 408  # Request Timeout
        
        # Force garbage collection on error
        gc.collect()
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/tables-for-device', methods=['GET'])
def tables_for_device_route():
    """
    Find all tables that have data for a given device_id
    
    Query Parameters:
        device_id (str, required): The device ID to search for
    
    Returns:
        200: List of tables with data for the device, including:
             - device_id: The searched device ID
             - device_uid: The corresponding device UID from device_lookup
             - tables_with_data: Array of tables containing data for this device
               - table: Table name
               - matched_by: Either 'device_id' or 'device_uuid'
             - count: Number of tables with data
        400: Missing device_id parameter
        404: Device not found in device_lookup table
        500: Database error
    """
    route_start_time = time.time()
    try:
        device_id = request.args.get('device_id')
        if not device_id:
            return jsonify({'error': 'missing device_id parameter'}), 400
        
        token_error = check_token()
        if token_error:
            return token_error
        
        # Get device_uuid from device_lookup table (column is device_uuid, not device_uid)
        success, device_lookup, _ = query_table('device_lookup', ['`device_uuid` = %s'], [device_id])
        device_uid = None
        if success and device_lookup.get('data') and len(device_lookup['data']) > 0:
            device_uid = device_lookup['data'][0].get('id')
        
        if not device_uid:
            elapsed = time.time() - route_start_time
            logger.warning(f"Device {device_id} not found in device_lookup table (took {elapsed:.3f}s)")
            return jsonify({'error': 'device_id not found', 'device_id': device_id}), 404
        
        # Get list of all tables
        success, all_tables, status_code = get_all_tables()
        if not success:
            return jsonify({'error': 'failed to retrieve table list'}), status_code
        
        tables_with_data = []
        
        # Check each table for data matching device_id or device_uid
        for table_name in all_tables:
            if table_name in ['device_lookup', 'aware_device', 'aware_log']:  # Skip device_lookup itself
                continue
            
            if not table_name.endswith('_transformed'):
                success, result, _ = query_table(table_name, ['`device_id` = %s'], [device_id], limit=1)
                if success and result.get('count', 0) > 0:
                    tables_with_data.append({
                        'table': table_name,
                        'matched_by': 'device_id'
                    })
                    continue
            
            else:
                if device_uid:
                    success, result, _ = query_table(table_name, ['`device_uid` = %s'], [device_uid], limit=1)
                    if success and result.get('count', 0) > 0:
                        # Remove "_transformed" suffix if present when matched by device_uid
                        display_table_name = table_name
                        if table_name.endswith('_transformed'):
                            display_table_name = table_name[:-len('_transformed')]
                        tables_with_data.append({
                            'table': display_table_name,
                            'matched_by': 'device_uid'
                        })
        
        response_data = {
            'device_id': device_id,
            'device_uid': device_uid,
            'tables_with_data': tables_with_data,
            'count': len(tables_with_data)
        }
        
        elapsed = time.time() - route_start_time
        logger.info(f"Found {len(tables_with_data)} tables with data for device {device_id} (uid: {device_uid}) in {elapsed:.3f}s")
        return jsonify(response_data), 200
    
    except Exception as e:
        elapsed = time.time() - route_start_time
        logger.error(f"Error in tables_for_device_route after {elapsed:.3f}s: {e}")
        return jsonify({'error': 'Internal server error'}), 500


def main():
    """Entry point for the aware-filter command"""
    logger.info("Starting AWARE Webservice Receiver")
    
    app.run(
        host='0.0.0.0',
        port=3446,
        ssl_context='adhoc',
        debug=False
    )


if __name__ == '__main__':
    main()

