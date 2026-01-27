# AWARE Filter API Summary

## Overview
The AWARE Filter service provides a lightweight API for inserting and retrieving sensor data from a MySQL database. It's designed to work with the AWARE framework for mobile and ubiquitous computing research.

## Data Structure

### Supported Table Types
The service manages two types of data tables:

1. **Double Values Tables** (e.g., `sensor_data`)
   - `device_id` (string): Device identifier
   - `timestamp` (integer): Unix timestamp in milliseconds
   - `double_value_0`, `double_value_1`, etc. (float): Sensor measurements
   - `accuracy` (integer): Measurement accuracy level

2. **Text Event Tables** (e.g., `text_events`)
   - `device_id` (string): Device identifier
   - `timestamp` (integer): Unix timestamp in milliseconds
   - `text_value` (string): Event or text data
   - Additional arbitrary string/integer columns as needed

## Core Modules

### 1. Authentication Module (`auth.py`)
Provides JWT-based authentication for API access.

**Functions:**
- `login(stats)` - Exchange study password for JWT token
  - **Input:** JSON with `password` field
  - **Output:** JWT token with configurable expiry (default 24 hours)
  - **Returns:** `(response_dict, status_code)`

- `check_token()` - Validate incoming JWT token from Authorization header
  - **Returns:** `None` if valid, or `(error_response, status_code)` if invalid
  - **Token Format:** Bearer token in `Authorization` header

**Environment Variables:**
- `TOKEN_SECRET`: Secret key for JWT signing
- `TOKEN_EXPIRY_HOURS`: Token validity duration (default: 24)
- `STUDY_PASSWORD`: Password for initial login

### 2. Insertion Module (`insertion.py`)
Handles data insertion into the database.

**Functions:**
- `insert_record(data, table_name, stats)` - Insert a single data record
  - **Parameters:**
    - `data` (dict): Record with column names as keys
    - `table_name` (string): Target table name
    - `stats` (dict): Statistics tracker with keys `successful_inserts`, `failed_inserts`
  - **Returns:** `(success: bool, message: string)`
  - **Updates:** `stats` dictionary with operation result

- `insert_records(data, table_name, stats)` - Insert single or multiple records
  - **Parameters:**
    - `data` (dict or list): Single record or list of records
    - `table_name` (string): Target table name
    - `stats` (dict): Statistics tracker
  - **Returns:** `(success: bool, response_dict: dict)`
  - **Response:** Contains `status`, `inserted` count, `errors` count, or error message

**Database Configuration (via environment variables):**
- `MYSQL_HOST`: Database host (default: localhost)
- `MYSQL_PORT`: Database port (default: 3306)
- `MYSQL_USER`: Database user (default: root)
- `MYSQL_PASSWORD`: Database password
- `MYSQL_DATABASE`: Database name (default: aware_database)

### 3. Retrieval Module (`retrieval.py`)
Retrieves sensor data from the database with filtering.

**Functions:**
- `query_data(table_name, device_id, device_uid, start_time, end_time)` - Query database
  - **Parameters:**
    - `table_name` (string): Table to query (required)
    - `device_id` (string): Filter by device ID (optional)
    - `device_uid` (string): Filter by device UID (optional)
    - `start_time` (integer): Unix timestamp in milliseconds (optional)
    - `end_time` (integer): Unix timestamp in milliseconds (optional)
  - **Requirements:** Either `device_id` or `device_uid` must be provided
  - **Returns:** `(success: bool, response_dict: dict, status_code: int)`
  - **Response Format:** 
    ```python
    {
        'count': int,        # Number of records returned
        'data': [...]        # List of records matching criteria
    }
    ```

## Error Handling

### Common Error Responses
- **400 Bad Request:** Missing required parameters
- **401 Unauthorized:** Invalid or missing token
- **500 Internal Server Error:** Database query/execution error
- **503 Service Unavailable:** Database connection failure

### Database Errors
- Connection failures return status 503 with descriptive error message
- Query execution errors return status 500 with error details
- All errors are logged for debugging

## Usage Example Flow

1. **Authentication:**
   - Call `login()` with study password to get JWT token
   - Include token in subsequent requests via `Authorization: Bearer <token>` header

2. **Data Insertion:**
   - Prepare data dictionary matching table schema
   - Call `insert_record()` or `insert_records()` with data and table name
   - Check response for success/failure status

3. **Data Retrieval:**
   - Call `query_data()` with table name and device filters
   - Optionally filter by time range using start_time and end_time
   - Response contains matching records with record count

## Logging
All operations are logged with appropriate levels:
- **INFO:** Successful operations
- **WARNING:** Failed authentication attempts
- **ERROR:** Database connection issues and query failures

Logger name: `aware_filter` (accessible via module-level logger instances)

## Statistics Tracking
The insertion functions update a provided stats dictionary:
```python
stats = {
    'successful_inserts': 0,
    'failed_inserts': 0,
    'unauthorized_attempts': 0  # For auth failures
}
```
This allows the calling application to track operation metrics.
