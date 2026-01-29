"""Pandas-backed in-memory DB backend used for testing."""

import re
import threading
import pandas as pd
from mysql.connector import Error


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
        m_cnt = re.match(r'select\s+count\s*\(\s*\*\s*\)\s+from\s+`?(\w+)`?(?:\s+limit\s+\d+)?', qi, re.I)
        if m_cnt:
            table = m_cnt.group(1)
            with self._conn._lock:
                df = self._conn._tables.get(table)
                count = int(df.shape[0]) if df is not None else 0
                self._results = [(count,)]
            return

        # SELECT * FROM table [WHERE ...] [LIMIT count] [OFFSET offset] or LIMIT offset,count
        m_sel = re.match(
            r"select\s+\*\s+from\s+`?(\w+)`?(?:\s+where\s+(.+?))?(?:\s+limit\s+(\d+)(?:\s*,\s*(\d+))?(?:\s+offset\s+(\d+))?)?",
            q,
            re.I,
        )
        if m_sel:
            table = m_sel.group(1)
            where_clause = m_sel.group(2)
            limit1 = m_sel.group(3)
            limit2 = m_sel.group(4)
            offset_kw = m_sel.group(5)

            with self._conn._lock:
                df = self._conn._tables.get(table)
                # Mimic MySQL: raise an error if table does not exist
                if df is None:
                    raise Error(f"Table '{table}' doesn't exist")

                if df.shape[0] == 0:
                    self._results = []
                    return

                # Apply optional WHERE clause (support simple `col` = %s AND ...)
                rows_df = df
                if where_clause:
                    cols = re.findall(r"`?(\w+)`?\s*=\s*%s", where_clause)
                    params_list = list(params) if params is not None else []

                    if not cols:
                        # Unsupported WHERE -> return no rows
                        self._results = []
                        return

                    mask = None
                    for i, col in enumerate(cols):
                        val = params_list[i] if i < len(params_list) else None
                        if isinstance(val, (bytes, bytearray)):
                            val = val.decode('utf-8', errors='ignore')

                        if col not in df.columns:
                            mask = pd.Series([False] * len(df)) if mask is None else (mask & pd.Series([False] * len(df)))
                            continue

                        col_series = df[col].astype(object)
                        col_mask = col_series == val
                        mask = col_mask if mask is None else (mask & col_mask)

                    if mask is None or not mask.any():
                        self._results = []
                        return

                    rows_df = df[mask]

                # Convert rows to tuples
                rows = [tuple(map(self._normalize_value, row)) for row in rows_df.itertuples(index=False, name=None)]

                # Determine offset and count
                if limit1 is None:
                    selected = rows
                else:
                    if limit2 is not None:
                        # LIMIT offset, count  (first is offset)
                        offset = int(limit1)
                        count = int(limit2)
                    else:
                        count = int(limit1)
                        offset = int(offset_kw) if offset_kw is not None else 0

                    if offset < 0:
                        offset = 0
                    if count < 0:
                        selected = []
                    else:
                        selected = rows[offset: offset + count]

                self._results = selected
            return

        # SELECT 1 FROM table [WHERE ...] [LIMIT ...] - used for existence checks
        m_exists = re.match(
            r"select\s+1\s+from\s+`?(\w+)`?(?:\s+where\s+(.+?))?(?:\s+limit\s+(\d+)(?:\s*,\s*(\d+))?)?",
            q,
            re.I,
        )
        if m_exists:
            table = m_exists.group(1)
            where_clause = m_exists.group(2)
            limit1 = m_exists.group(3)
            limit2 = m_exists.group(4)

            with self._conn._lock:
                df = self._conn._tables.get(table)
                # Mimic MySQL: raise an error if table does not exist
                if df is None:
                    raise Error(f"Table '{table}' doesn't exist")

                # If table exists but empty, return no rows
                if df.shape[0] == 0:
                    self._results = []
                    return

                # No WHERE: just check for at least one row
                if not where_clause:
                    # Respect LIMIT presence but existence only requires at least one row
                    self._results = [(1,)] if df.shape[0] > 0 else []
                    return

                # Simple WHERE parsing: support patterns like `col` = %s joined with AND
                cols = re.findall(r"`?(\w+)`?\s*=\s*%s", where_clause)
                params_list = list(params) if params is not None else []

                if not cols:
                    # Unsupported WHERE clause form -> no match
                    self._results = []
                    return

                # Build boolean mask for rows matching all equality conditions
                mask = None
                for i, col in enumerate(cols):
                    val = params_list[i] if i < len(params_list) else None
                    if isinstance(val, (bytes, bytearray)):
                        val = val.decode('utf-8', errors='ignore')

                    if col not in df.columns:
                        # Column missing -> no rows match
                        mask = pd.Series([False] * len(df)) if mask is None else mask & pd.Series([False] * len(df))
                        continue

                    col_series = df[col].astype(object)
                    col_mask = col_series == val
                    mask = col_mask if mask is None else (mask & col_mask)

                if mask is None or not mask.any():
                    self._results = []
                    return

                # There is at least one matching row; honor LIMIT semantics
                matched = df[mask]
                rows = [tuple(map(self._normalize_value, row)) for row in matched.itertuples(index=False, name=None)]

                if limit1 is not None:
                    if limit2 is not None:
                        offset = int(limit1)
                        count = int(limit2)
                    else:
                        count = int(limit1)
                        offset = 0

                    if offset < 0:
                        offset = 0
                    if count < 0:
                        rows = []
                    else:
                        rows = rows[offset: offset + count]

                self._results = [(1,)] if rows else []
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
