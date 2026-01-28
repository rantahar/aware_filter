"""Database connection management module"""

import mysql.connector
from mysql.connector import Error
import logging
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DATABASE', 'aware_database'),
}

# Module-level persistent connection
_connection = None


def get_connection():
    """Get the persistent database connection, reconnecting if necessary."""
    global _connection
    
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
        except Error as e:
            logger.error(f"Error closing database connection: {e}")
        finally:
            _connection = None
