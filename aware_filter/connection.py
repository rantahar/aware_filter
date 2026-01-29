"""Database connection management module"""

import logging
import os
import atexit
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import re
import threading
import pandas as pd

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


class PandasCursor:
    def __init__(self, conn):
        self._conn = conn
        self._results = []
        self._lastrowid = None

    def _normalize_value(self, v):
        if isinstance(v, (bytes, bytearray)):
            return v.decode('utf-8', errors='ignore')
        return v

    def execute(self, query, params=None):
        q = (query or "").strip()
        qi = q.lower()

        # INSERT INTO <table> ... VALUES
        m_ins = re.match(r"insert\s+into\s+`?(\w+)`?(?:\s*\([^)]*\))?\s+values", qi, re.I)
        if m_ins and params is not None:
            table = m_ins.group(1)
            if pd is None:
                raise RuntimeError('pandas is required for memory DB backend')

            if isinstance(params, (list, tuple)):
                row = [self._normalize_value(v) for v in params]
            else:
                row = [self._normalize_value(params)]

            with self._conn._lock:
                df = self._conn._tables.get(table)
                if df is None:
                    cols = [f'col{i+1}' for i in range(len(row))]
                    df = pd.DataFrame([row], columns=cols)
                    df.index = pd.RangeIndex(start=1, stop=2)
                    self._conn._tables[table] = df
                    last_id = 1
                else:
                    if df.shape[1] < len(row):
                        extra = len(row) - df.shape[1]
                        for i in range(extra):
                            df[f'col{df.shape[1] + i + 1}'] = None
                    new_idx = int(df.index.max()) + 1 if len(df) > 0 else 1
                    new_row = pd.DataFrame([row], columns=df.columns)
                    new_row.index = pd.Index([new_idx])
                    df = pd.concat([df, new_row])
                    self._conn._tables[table] = df
                    last_id = new_idx

                self._lastrowid = last_id
                self._results = []
            return

        # SELECT COUNT(*) FROM table
        m_cnt = re.match(r'select\s+count\s*\(\s*\*\s*\)\s+from\s+`?(\w+)`?', qi, re.I)
        if m_cnt:
            table = m_cnt.group(1)
            with self._conn._lock:
                df = self._conn._tables.get(table)
                count = int(df.shape[0]) if df is not None else 0
                self._results = [(count,)]
            return

        # SELECT * FROM table
        m_sel = re.match(r'select\s+\*\s+from\s+`?(\w+)`?', qi, re.I)
        if m_sel:
            table = m_sel.group(1)
            with self._conn._lock:
                df = self._conn._tables.get(table)
                if df is None or df.shape[0] == 0:
                    self._results = []
                else:
                    self._results = [tuple(map(self._normalize_value, row)) for row in df.itertuples(index=False, name=None)]
            return

        raise NotImplementedError(f"Query not supported by memory backend: {q}")

    def fetchone(self):
        return self._results[0] if self._results else None

    def fetchall(self):
        return list(self._results)

    @property
    def lastrowid(self):
        return self._lastrowid

    def close(self):
        self._results = []


class PandasConnection:
    def __init__(self):
        if pd is None:
            raise RuntimeError('pandas is required for memory DB backend')
        self._tables = {}
        self._lock = threading.Lock()

    def cursor(self):
        return PandasCursor(self)

    def commit(self):
        return

    def ping(self, reconnect=False, attempts=1, delay=0):
        return True

    def close(self):
        with self._lock:
            self._tables.clear()


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
