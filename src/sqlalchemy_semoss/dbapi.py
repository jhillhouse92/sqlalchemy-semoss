"""PEP 249 DB-API 2.0 driver for SEMOSS databases.

Wraps ``ai_server.DatabaseEngine`` in a standard Python database interface.

Usage::

    from sqlalchemy_semoss import connect

    conn = connect(engine_id="your-engine-uuid")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    for row in cursor.fetchall():
        print(row)
"""

import datetime

from .exceptions import InterfaceError, ProgrammingError
from .router import SqlRouter
from .types import STRING

# PEP 249 module-level constants
apilevel = "2.0"
threadsafety = 1
paramstyle = "format"


def connect(engine_id, insight_id=None, **kwargs):
    """Create a new DB-API connection to a SEMOSS database engine.

    Args:
        engine_id: The SEMOSS database engine UUID.
        insight_id: Optional insight ID. If ``None``, omitted from the
            ``DatabaseEngine`` constructor.
        **kwargs: Reserved for forward compatibility.

    Returns:
        A :class:`SemossConnection` instance.
    """
    return SemossConnection(engine_id=engine_id, insight_id=insight_id, **kwargs)


class SemossConnection:
    """DB-API 2.0 Connection wrapping a SEMOSS ``DatabaseEngine``.

    The underlying ``DatabaseEngine`` is created lazily on first use
    (since ``ai_server`` is only available at runtime in the SEMOSS
    environment).

    Transaction semantics: SEMOSS auto-commits each operation.
    ``commit()`` and ``rollback()`` are no-ops.
    """

    def __init__(self, engine_id, insight_id=None, **kwargs):
        self._engine_id = engine_id
        self._insight_id = insight_id
        self._database = None
        self._closed = False

    @property
    def database(self):
        """The underlying ``DatabaseEngine`` instance (lazy-loaded)."""
        if self._database is None:
            from ai_server import DatabaseEngine

            kwargs = {"engine_id": self._engine_id}
            if self._insight_id is not None:
                kwargs["insight_id"] = self._insight_id

            self._database = DatabaseEngine(**kwargs)
        return self._database

    def close(self):
        self._closed = True

    def commit(self):
        """No-op — SEMOSS auto-commits each operation."""
        self._check_closed()

    def rollback(self):
        """No-op — SEMOSS RPC calls cannot be rolled back."""
        self._check_closed()

    def cursor(self):
        """Return a new :class:`SemossCursor` bound to this connection."""
        self._check_closed()
        return SemossCursor(self)

    def _check_closed(self):
        if self._closed:
            raise InterfaceError("Connection is closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class SemossCursor:
    """DB-API 2.0 Cursor that executes SQL via a SEMOSS ``DatabaseEngine``.

    Results are fetched eagerly — the entire result set comes back in one
    RPC call, then the cursor iterates over the in-memory data.
    """

    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self.rowcount = -1
        self.arraysize = 1
        self._result_rows = []
        self._result_index = 0
        self._closed = False

    def execute(self, operation, parameters=None):
        """Execute a SQL statement.

        Parameters are interpolated into the SQL string before sending,
        since ``DatabaseEngine`` methods accept only final SQL strings.
        """
        self._check_closed()
        self._reset()

        if parameters:
            operation = self._interpolate(operation, parameters)

        database = self.connection.database
        kind = SqlRouter.classify(operation)

        try:
            raw_result = SqlRouter.execute(database, operation)
        except Exception as e:
            if kind in ("ddl", "insert", "update", "delete"):
                self.description = ()
                self.rowcount = -1
                self._result_rows = []
                return
            raise ProgrammingError(str(e)) from e

        if kind in ("ddl", "insert", "update", "delete"):
            self.description = ()
            self._result_rows = []
            if isinstance(raw_result, dict):
                self.rowcount = raw_result.get(
                    "rowsAffected",
                    raw_result.get("modifiedCount", -1),
                )
            elif isinstance(raw_result, (int, float)):
                self.rowcount = int(raw_result)
            else:
                self.rowcount = -1
            return

        self._process_result(raw_result, kind)

    def executemany(self, operation, seq_of_parameters):
        """Execute the operation for each parameter set in the sequence."""
        self._check_closed()
        total_rows = 0
        for params in seq_of_parameters:
            self.execute(operation, params)
            if self.rowcount > 0:
                total_rows += self.rowcount
        self.rowcount = total_rows

    def fetchone(self):
        """Fetch the next row, or ``None`` if no more rows."""
        self._check_closed()
        if self._result_index >= len(self._result_rows):
            return None
        row = self._result_rows[self._result_index]
        self._result_index += 1
        return row

    def fetchmany(self, size=None):
        """Fetch the next *size* rows as a list."""
        self._check_closed()
        if size is None:
            size = self.arraysize
        end = min(self._result_index + size, len(self._result_rows))
        rows = self._result_rows[self._result_index:end]
        self._result_index = end
        return rows

    def fetchall(self):
        """Fetch all remaining rows as a list."""
        self._check_closed()
        rows = self._result_rows[self._result_index:]
        self._result_index = len(self._result_rows)
        return rows

    def close(self):
        self._closed = True
        self._result_rows = []

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    # --- Internal helpers ---

    def _reset(self):
        self.description = None
        self.rowcount = -1
        self._result_rows = []
        self._result_index = 0

    def _interpolate(self, sql, parameters):
        if isinstance(parameters, dict):
            escaped = {k: self._escape(v) for k, v in parameters.items()}
            return sql % escaped
        else:
            escaped = tuple(self._escape(v) for v in parameters)
            return sql % escaped

    @staticmethod
    def _escape(value):
        """Escape a Python value for safe SQL interpolation."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        if isinstance(value, bytes):
            return "E'\\\\x" + value.hex() + "'"
        if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
            return "'" + str(value) + "'"
        return "'" + str(value).replace("'", "''") + "'"

    def _process_result(self, raw_result, kind):
        if kind == "select":
            rows, columns = self._parse_select_result(raw_result)
            self._result_rows = rows
            if columns:
                self.description = tuple(
                    (col, STRING, None, None, None, None, None)
                    for col in columns
                )
            self.rowcount = len(rows)
        else:
            if isinstance(raw_result, dict):
                self.rowcount = raw_result.get(
                    "rowsAffected",
                    raw_result.get("modifiedCount", -1),
                )
            elif isinstance(raw_result, (int, float)):
                self.rowcount = int(raw_result)
            else:
                self.rowcount = -1

    def _parse_select_result(self, raw_result):
        """Parse a SELECT response into ``(rows, columns)``.

        Handles multiple response shapes from ``DatabaseEngine.execQuery``:
        list of dicts, dict with ``data`` key, dict with ``headers``/``values``,
        and pandas DataFrames.
        """
        if raw_result is None:
            return [], []

        if isinstance(raw_result, list):
            if len(raw_result) > 0 and isinstance(raw_result[0], dict):
                columns = list(raw_result[0].keys())
                rows = [tuple(row.get(c) for c in columns) for row in raw_result]
                return rows, columns
            return [], []

        if isinstance(raw_result, dict) and "data" in raw_result:
            return self._parse_select_result(raw_result["data"])

        if isinstance(raw_result, dict) and "headers" in raw_result and "values" in raw_result:
            columns = raw_result["headers"]
            rows = [tuple(row) for row in raw_result["values"]]
            return rows, columns

        try:
            import pandas as pd
            if isinstance(raw_result, pd.DataFrame):
                columns = list(raw_result.columns)
                rows = [tuple(row) for row in raw_result.itertuples(index=False)]
                return rows, columns
        except ImportError:
            pass

        return [], []

    def _check_closed(self):
        if self._closed:
            raise InterfaceError("Cursor is closed")

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
