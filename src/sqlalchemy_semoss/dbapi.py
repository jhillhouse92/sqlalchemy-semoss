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
import re

from .exceptions import InterfaceError, ProgrammingError
from .router import SqlRouter
from .types import STRING

# PEP 249 module-level constants
apilevel = "2.0"
threadsafety = 1
paramstyle = "format"

# ---------------------------------------------------------------------------
# SQL column-order parser
# ---------------------------------------------------------------------------
# Standard DB-API drivers receive column order from the database wire protocol.
# Because SEMOSS returns JSON dicts (unordered relative to the SQL), we parse
# the SQL SELECT/RETURNING clause to recover the authoritative column order.

_SELECT_COLS_RE = re.compile(
    r"^\s*SELECT\s+(?:DISTINCT\s+)?(.*?)\s+FROM\s",
    re.IGNORECASE | re.DOTALL,
)
_RETURNING_COLS_RE = re.compile(
    r"\bRETURNING\s+(.+)$",
    re.IGNORECASE | re.DOTALL,
)


def _split_columns_paren_aware(col_string):
    """Split a column list by commas, respecting parenthesis depth."""
    columns = []
    current = []
    depth = 0
    for char in col_string:
        if char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            columns.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    if current:
        columns.append("".join(current).strip())
    return columns


def _extract_column_name(col_expr):
    """Extract the output column name from a single column expression.

    Returns the alias (if AS is used), the bare column name (stripping any
    table qualifier), or ``None`` if the name cannot be determined (e.g.
    ``*`` or a bare expression without an alias).
    """
    expr = col_expr.strip()

    if expr == "*" or expr.endswith(".*"):
        return None

    # Match: ... AS alias  (with optional quoting)
    as_match = re.search(r"\bAS\s+\"?(\w+)\"?\s*$", expr, re.IGNORECASE)
    if as_match:
        return as_match.group(1)

    # Simple identifier or table.column
    simple_match = re.match(r"^(?:\w+\.)?(\w+)$", expr)
    if simple_match:
        return simple_match.group(1)

    # Expression without alias (e.g. count(*), 1+1) — can't determine name
    return None


def _parse_column_order(sql):
    """Parse expected column names from a SQL statement.

    Returns a list of column name strings in declaration order, or ``None``
    if the order cannot be reliably determined (SELECT *, expressions without
    aliases, or unparseable SQL).
    """
    sql_stripped = sql.strip().rstrip(";")

    # Try RETURNING clause first (takes priority for INSERT/UPDATE/DELETE RETURNING)
    ret_match = _RETURNING_COLS_RE.search(sql_stripped)
    if ret_match:
        col_string = ret_match.group(1).strip()
    else:
        sel_match = _SELECT_COLS_RE.match(sql_stripped)
        if sel_match:
            col_string = sel_match.group(1).strip()
        else:
            return None

    raw_cols = _split_columns_paren_aware(col_string)

    names = []
    for col_expr in raw_cols:
        name = _extract_column_name(col_expr)
        if name is None:
            return None
        names.append(name)

    return names if names else None


# ---------------------------------------------------------------------------
# Value type coercion
# ---------------------------------------------------------------------------
# SEMOSS returns all values as strings (DataFrame with object dtype).
# This function infers the native Python type from the string representation.

_ISO_DATE_LEN = 10  # "YYYY-MM-DD"
_ISO_TS_MIN_LEN = 19  # "YYYY-MM-DD HH:MM:SS"


def _coerce_value(value):
    """Convert a string value from SEMOSS to its native Python type.

    Conversion rules (applied in order):
    - None / non-string → returned unchanged
    - "true" / "false" (case-insensitive) → bool
    - Integer pattern (no dot, no 'e') → int
    - Numeric pattern → float
    - "YYYY-MM-DD HH:MM:SS" → datetime.datetime
    - "YYYY-MM-DD" → datetime.date
    - Everything else → str (unchanged)
    """
    if value is None:
        return None
    if not isinstance(value, str):
        return value

    # Boolean
    low = value.lower()
    if low == "true":
        return True
    if low == "false":
        return False

    # Integer (no decimal point, no exponent notation)
    if "." not in value and "e" not in low:
        try:
            return int(value)
        except (ValueError, OverflowError):
            pass

    # Float
    try:
        return float(value)
    except (ValueError, OverflowError):
        pass

    # Timestamp: "YYYY-MM-DD HH:MM:SS[.ffffff]"
    if len(value) >= _ISO_TS_MIN_LEN and value[4] == "-" and value[10] == " ":
        try:
            return datetime.datetime.fromisoformat(value)
        except ValueError:
            pass

    # Date: "YYYY-MM-DD"
    if len(value) == _ISO_DATE_LEN and value[4] == "-" and value[7] == "-":
        try:
            return datetime.date.fromisoformat(value)
        except ValueError:
            pass

    return value


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

        self._process_result(raw_result, kind, operation)

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
        # Handle pandas/numpy NA-like sentinels (NaT, NaN, NA)
        try:
            import pandas as pd
            if isinstance(value, type(pd.NaT)) or value is pd.NaT:
                return "NULL"
            if pd.isna(value):
                return "NULL"
        except (ImportError, TypeError, ValueError):
            pass
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

    def _process_result(self, raw_result, kind, sql=None):
        if kind == "select":
            rows, columns = self._parse_select_result(raw_result, sql)
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

    def _parse_select_result(self, raw_result, sql=None):
        """Parse a SELECT response into ``(rows, columns)``.

        Handles multiple response shapes from ``DatabaseEngine.execQuery``:
        list of dicts, dict with ``data`` key, dict with ``headers``/``values``,
        and pandas DataFrames.

        When *sql* is provided, the SELECT/RETURNING column list is parsed
        to determine the authoritative column order.  If parsing fails or
        columns don't match the response keys, dict key order is used as
        fallback.
        """
        if raw_result is None:
            return [], []

        if isinstance(raw_result, list):
            if len(raw_result) > 0 and isinstance(raw_result[0], dict):
                columns = list(raw_result[0].keys())
                # Attempt to reorder based on the SQL column list
                if sql is not None:
                    parsed_order = _parse_column_order(sql)
                    if parsed_order is not None:
                        response_keys = set(raw_result[0].keys())
                        if all(col in response_keys for col in parsed_order):
                            columns = parsed_order
                rows = [
                    tuple(_coerce_value(row.get(c)) for c in columns)
                    for row in raw_result
                ]
                return rows, columns
            return [], []

        if isinstance(raw_result, dict) and "data" in raw_result:
            return self._parse_select_result(raw_result["data"], sql)

        if isinstance(raw_result, dict) and "headers" in raw_result and "values" in raw_result:
            columns = raw_result["headers"]
            rows = [
                tuple(_coerce_value(v) for v in row)
                for row in raw_result["values"]
            ]
            return rows, columns

        try:
            import pandas as pd
            if isinstance(raw_result, pd.DataFrame):
                # Replace pandas NA sentinels (NaT, NaN, NA) with Python None
                # so downstream code sees standard None instead of pd.NaT/NaN
                df = raw_result.where(raw_result.notna(), other=None)
                columns = list(df.columns)
                # Attempt to reorder based on the SQL column list
                if sql is not None:
                    parsed_order = _parse_column_order(sql)
                    if parsed_order is not None:
                        df_cols = set(df.columns)
                        if all(col in df_cols for col in parsed_order):
                            df = df[parsed_order]
                            columns = parsed_order
                rows = [
                    tuple(_coerce_value(v) if v is not None else None
                          for v in row)
                    for row in df.itertuples(index=False)
                ]
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
