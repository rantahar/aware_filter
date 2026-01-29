"""Database connection management module"""

import logging
import os
import atexit
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import re
import threading
from .pandas_backend import PandasCursor, PandasConnection

logger = logging.getLogger(__name__)

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DATABASE', 'aware_database'),
}

DB_BACKEND = os.getenv('DB_BACKEND', 'mysql').lower()  # BACKEND: 'mysql' (default) or 'memory' (in-memory pandas DataFrames)

# Module-level persistent connection
_connection = None


        


def get_connection():
    """Get the persistent database connection, reconnecting if necessary.

    When `DB_BACKEND` is set to 'memory' this returns an in-memory pandas-backed
    connection for local testing. Otherwise it attempts to connect to a real
    MySQL server using `mysql.connector`.
    """
    global _connection

    if DB_BACKEND in ('memory', 'pandas', 'inmemory'):
        if _connection is None or not isinstance(_connection, PandasConnection):
            _connection = PandasConnection()
            logger.info("Using in-memory pandas DB backend for testing")
        return _connection

    # If connection doesn't exist, create it
    if _connection is None:
        try:
            _connection = mysql.connector.connect(**DB_CONFIG)
            logger.info("Database connection established")
        except Error as e:
            logger.error(f"Error connecting to database: {e}")
            return None

    # Check if connection is still alive, reconnect if not
    try:
        _connection.ping(reconnect=True, attempts=1, delay=0)
    except Error as e:
        logger.warning(f"Connection lost, reconnecting: {e}")
        try:
            _connection = mysql.connector.connect(**DB_CONFIG)
            logger.info("Database connection re-established")
        except Error as e:
            logger.error(f"Error reconnecting to database: {e}")
            _connection = None
            return None

    return _connection


def close_connection():
    """Close the persistent database connection."""
    global _connection
    
    if _connection is not None:
        try:
            _connection.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")
        finally:
            _connection = None


# Register cleanup to run only on actual app shutdown
atexit.register(close_connection)
