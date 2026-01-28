"""Data insertion module for AWARE Webservice Receiver"""

from mysql.connector import Error
import logging
from dotenv import load_dotenv
import os
from .connection import get_connection

logger = logging.getLogger(__name__)

load_dotenv()

STUDY_PASSWORD = os.getenv('STUDY_PASSWORD', 'aware_study_password')


def insert_record(data, table_name, stats):
    """Insert a single record into the database."""
    conn = get_connection()
    if conn is None:
        return False, "Database connection failed"
    
    try:
        cursor = conn.cursor()

        # Build INSERT query
        columns = ', '.join(f'`{key}`' for key in data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

        cursor.execute(query, list(data.values()))
        conn.commit()

        logger.info(f"Data inserted successfully into {table_name}")
        stats['successful_inserts'] += 1
        return True, "Data inserted successfully"

    except Error as e:
        logger.error(f"Error inserting data: {e}")
        stats['failed_inserts'] += 1
        return False, str(e)
    finally:
        cursor.close()


def insert_records(data, table_name, stats):
    """
    Insert records into the database.
    Handles both single records and batches.
    
    Args:
        data: Either a single dict or list of dicts to insert
        table_name: Name of the table to insert into
        stats: Statistics dictionary to update
    
    Returns:
        tuple: (success: bool, response_dict: dict)
    """
    if not data:
        return False, {'error': 'no data'}
    
    # Handle both single object and array of objects
    if isinstance(data, list):
        logger.info(f"Received {len(data)} records for table: {table_name}")
        success_count = 0
        error_count = 0
        
        for record in data:
            success, msg = insert_record(record, table_name, stats)
            if success:
                success_count += 1
            else:
                error_count += 1
                logger.error(f"Failed to insert record: {msg}")
        
        return True, {
            'status': 'ok',
            'inserted': success_count,
            'errors': error_count
        }
    else:
        # Single record
        logger.info(f"Received 1 record for table: {table_name}")
        success, msg = insert_record(data, table_name, stats)
        
        if success:
            return True, {'status': 'ok'}
        else:
            return False, {'error': msg}
