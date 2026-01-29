#!/usr/bin/env python3
"""
Flask endpoints moved out of package main to avoid enabling them by default.
Use `create_app()` or `run_server()` to start the HTTP endpoints explicitly.
"""

from flask import Flask, jsonify, request
import logging
import gc
import time
from datetime import datetime
import os

from .auth import login, check_token
from .insertion import insert_records, STUDY_PASSWORD
from .retrieval import query_table, get_all_tables, table_has_data, query_data, get_tables_for_devices
from .connection import get_connection

from .utils import check_memory_usage, stats, logger


app = Flask(__name__)


@app.route('/webservice/index/<study_id>/<password>/<table_name>', methods=['POST'])
def webservice_table_route(study_id, password, table_name):
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
    route_start_time = time.time()
    result = login(stats)
    elapsed = time.time() - route_start_time
    logger.info(f"login endpoint completed in {elapsed:.3f}s")
    return result


@app.route('/data', methods=['GET'])
def query_route():
    request_start_time = datetime.utcnow()
    try:
        memory_mb = check_memory_usage()
        logger.debug(f"Processing data query request. Current memory: {memory_mb:.1f} MB")

        token_error = check_token()
        if token_error:
            return token_error

        table_name = request.args.get('table')
        if not table_name:
            return jsonify({'error': 'missing table parameter'}), 400

        success, response_dict, status_code = query_data(table_name, request.args)

        if not success:
            request_duration = (datetime.utcnow() - request_start_time).total_seconds()
            logger.error(f"Query failed with status {status_code} after {request_duration:.1f}s")
            return jsonify(response_dict), status_code

        request_duration = (datetime.utcnow() - request_start_time).total_seconds()

        memory_after_query = check_memory_usage()
        logger.debug(f"After database query. Memory: {memory_after_query:.1f} MB, Duration: {request_duration:.1f}s")

        warnings = []
        if response_dict['total_count'] > 100000:
            warnings.append(f"Large dataset ({response_dict['total_count']} total records). Consider using pagination with limit and offset parameters.")

        if request_duration > 60:
            warnings.append(f"Long-running query ({request_duration:.1f}s). Consider adding more specific filters or pagination.")
            logger.warning(f"Long query duration: {request_duration:.1f}s for table {table_name}")

        if warnings:
            response_dict['warnings'] = warnings

        response_dict['query_duration_seconds'] = round(request_duration, 2)

        return jsonify(response_dict), 200

    except Exception as e:
        request_duration = (datetime.utcnow() - request_start_time).total_seconds()
        logger.error(f"Unexpected error in query route after {request_duration:.1f}s: {e}")

        if request_duration > 240:
            logger.error(f"Query likely timed out after {request_duration:.1f}s. Consider using pagination or more specific filters.")
            return jsonify({
                'error': 'Query timeout - request took too long to process',
                'suggestion': 'Use limit/offset parameters or more specific filters to reduce dataset size',
                'duration_seconds': round(request_duration, 2)
            }), 408

        gc.collect()
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/tables-for-device', methods=['GET'])
def tables_for_device_route():
    route_start_time = time.time()
    try:
        device_id_param = request.args.get('device_id')
        if not device_id_param:
            return jsonify({'error': 'missing device_id parameter'}), 400

        token_error = check_token()
        if token_error:
            return token_error

        requested_device_ids = [d.strip() for d in device_id_param.split(',') if d.strip()]

        success, response_dict, status_code = get_tables_for_devices(requested_device_ids)

        elapsed = time.time() - route_start_time
        if success:
            logger.info(f"Found {response_dict['count']} tables with data for {len(requested_device_ids)} devices in {elapsed:.3f}s")
        else:
            logger.warning(f"tables_for_device_route failed with status {status_code} after {elapsed:.3f}s")

        return jsonify(response_dict), status_code

    except Exception as e:
        elapsed = time.time() - route_start_time
        logger.error(f"Error in tables_for_device_route after {elapsed:.3f}s: {e}")
        return jsonify({'error': 'Internal server error'}), 500


def create_app():
    return app


def run_server():
    port = int(os.getenv('API_PORT', 3446))
    logger.info('Starting Flask endpoints on port %s', port)
    app.run(host='0.0.0.0', port=port, ssl_context='adhoc', debug=False)
