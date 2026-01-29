"""Data insertion module for AWARE Webservice Receiver"""

from mysql.connector import Error
import logging
from dotenv import load_dotenv
import os
from .connection import get_connection

logger = logging.getLogger(__name__)

load_dotenv()

STUDY_PASSWORD = os.getenv('STUDY_PASSWORD', 'aware_study_password')

# Limit collected data. This does not always work on client side.
# Expressed in microseconds.
general_rate_limit = 200000 # 5 Hz
# Specific limits per table. (overrides general_rate_limit)
rate_limits = {
    'accelerometer': 200000,  # 5 Hz
}


def apply_rate_limit(data, table_name):
    """
    Apply the rate limit to incoming data.

    Args:
        data: Single record dict or list of records (dicts) with 'timestamp' field
        table_name: Name of the table to determine specific rate limit
    Returns:
        Filtered list of records (or single record dict) adhering to the rate limit
    """
    # Handle single record dict - no rate limiting needed for single records
    if isinstance(data, dict):
        return data
    
    # Handle list of records
    limit = rate_limits.get(table_name, general_rate_limit)
    
    last_timestamp = None
    filtered_data = []
    for record in data:
        timestamp = record.get('timestamp')
        if last_timestamp is None or (timestamp - last_timestamp) >= limit:
            filtered_data.append(record)
            last_timestamp = timestamp
    return filtered_data


def get_device_uid(device_id):
    """
    Look up device_uid from device_lookup table using device_id (device_uuid).
    
    Args:
        device_id: The device UUID/ID to look up
    
    Returns:
        tuple: (success: bool, device_uid: str or None, error_message: str or None)
    """
    conn = get_connection()
    if conn is None:
        return False, None, "Database connection failed"
    
    try:
        cursor = conn.cursor(dictionary=True)
        query = "SELECT `id` FROM `device_lookup` WHERE `device_uuid` = %s LIMIT 1"
        cursor.execute(query, [device_id])
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            device_uid = result.get('id')
            logger.debug(f"Found device_uid {device_uid} for device_id {device_id}")
            return True, device_uid, None
        else:
            logger.warning(f"Device lookup failed: device_id {device_id} not found in device_lookup table")
            return False, None, f"Device {device_id} not found in device_lookup"
    
    except Error as e:
        logger.error(f"Error looking up device: {e}")
        return False, None, str(e)


def transform_and_write(record, original_table_name, stats):
    """
    Transform a record by replacing device_id with device_uid and write to the transformed table.
    
    This function:
    1. Checks if a transformed table exists (original_table_name + "_transformed")
    2. Looks up the device_uid for the record's device_id
    3. Creates a new record with device_uid instead of device_id
    4. Inserts the transformed record into the transformed table
    
    Args:
        record: The original record dict containing device_id
        original_table_name: Name of the original table (e.g., 'sensor_data')
        stats: Statistics dictionary to update
    
    Returns:
        tuple: (success: bool, error_message: str or None)
    """
    # Only transform if the record has a device_id field
    if 'device_id' not in record:
        logger.debug(f"Record has no device_id field, skipping transformation for table {original_table_name}")
        return True, None
    
    transformed_table_name = f"{original_table_name}_transformed"
    
    # Check if transformed table exists by trying to query it
    conn = get_connection()
    if conn is None:
        logger.warning(f"Cannot transform record: database connection failed")
        return False, "Database connection failed"
    
    try:
        cursor = conn.cursor()
        # Try to check if transformed table exists
        cursor.execute(f"SELECT 1 FROM `{transformed_table_name}` LIMIT 1")
        cursor.close()
    except Error:
        # Table doesn't exist, no transformation needed
        logger.debug(f"Transformed table {transformed_table_name} does not exist, skipping transformation")
        return False, "Transformed table does not exist"
    
    # Look up device_uid for this device_id
    success, device_uid, error_msg = get_device_uid(record['device_id'])
    if not success:
        logger.warning(f"Cannot transform record for table {original_table_name}: {error_msg}")
        stats['transformation_failures'] = stats.get('transformation_failures', 0) + 1
        # Don't fail the original insert, just log the warning
        return False, error_msg
    
    # Create transformed record: copy all fields except device_id, add device_uid
    transformed_record = {k: v for k, v in record.items() if k != 'device_id'}
    transformed_record['device_uid'] = device_uid
    
    # Insert into transformed table
    try:
        cursor = conn.cursor()
        columns = ', '.join(f'`{key}`' for key in transformed_record.keys())
        placeholders = ', '.join(['%s'] * len(transformed_record))
        query = f"INSERT INTO `{transformed_table_name}` ({columns}) VALUES ({placeholders})"
        
        cursor.execute(query, list(transformed_record.values()))
        conn.commit()
        cursor.close()
        
        logger.info(f"Transformed record written successfully to {transformed_table_name}")
        stats['successful_transforms'] = stats.get('successful_transforms', 0) + 1
        return True, None
    
    except Error as e:
        logger.error(f"Error writing transformed record to {transformed_table_name}: {e}")
        stats['transformation_failures'] = stats.get('transformation_failures', 0) + 1
        # Don't fail the original insert, just log the error
        return False, str(e)


def insert_record(data, table_name, stats):
    """
    Insert a single record into the database.

    If a transformed table exists, use that table instead.
    
    Args:
        data: Record dict to insert
        table_name: Name of the table to insert into
        stats: Statistics dictionary to update
    
    Returns:
        tuple: (success: bool, message: str)
    """
    conn = get_connection()
    if conn is None:
        return False, "Database connection failed"
    
    try:
        # Attempt to insert into a transformed table
        transform_success, transform_error = transform_and_write(data, table_name, stats)

        if transform_success:
            # If transformation succeeded, we consider the insert done
            return True, "Data inserted successfully into transformed table"

        cursor = conn.cursor()

        # Build INSERT query
        columns = ', '.join(f'`{key}`' for key in data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

        cursor.execute(query, list(data.values()))
        conn.commit()
        cursor.close()

        logger.info(f"Data inserted successfully into {table_name}")
        stats['successful_inserts'] += 1

        return True, "Data inserted successfully"

    except Error as e:
        logger.error(f"Error inserting data: {e}")
        stats['failed_inserts'] += 1
        return False, str(e)


def insert_records(data, table_name, stats):
    """
    Insert records into the database.
    
    Args:
        data: Either a single dict or list of dicts to insert
        table_name: Name of the table to insert into
        stats: Statistics dictionary to update
    
    Returns:
        tuple: (success: bool, response_dict: dict)
    """
    if not data:
        return False, {'error': 'no data'}
    
    data = apply_rate_limit(data, table_name)
    
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
