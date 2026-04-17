"""SQL statement classification and routing.

Classifies SQL statements by type (SELECT, INSERT, UPDATE, DELETE, DDL) and
dispatches them to the correct ``ai_server.DatabaseEngine`` method.

The SEMOSS ``DatabaseEngine`` exposes four methods:

- ``execQuery(query=...)`` — for SELECT, DDL, and DML with RETURNING
- ``insertData(query=...)`` — for INSERT (without RETURNING)
- ``updateData(query=...)`` — for UPDATE
- ``removeData(query=...)`` — for DELETE

DML statements with a ``RETURNING`` clause are routed through ``execQuery``
since they return a result set.
"""

import re

_SELECT_RE = re.compile(r"^\s*(SELECT|WITH|EXPLAIN|SHOW|DESCRIBE)\b", re.IGNORECASE)
_INSERT_RE = re.compile(r"^\s*INSERT\b", re.IGNORECASE)
_UPDATE_RE = re.compile(r"^\s*UPDATE\b", re.IGNORECASE)
_DELETE_RE = re.compile(r"^\s*(DELETE|TRUNCATE)\b", re.IGNORECASE)
_DDL_RE = re.compile(r"^\s*(CREATE|ALTER|DROP|GRANT|REVOKE)\b", re.IGNORECASE)
_RETURNING_RE = re.compile(r"\bRETURNING\b", re.IGNORECASE)


class SqlRouter:
    """Classifies SQL statements and dispatches them to the appropriate
    ``DatabaseEngine`` method.
    """

    @staticmethod
    def classify(sql):
        """Classify a SQL statement.

        DML with a ``RETURNING`` clause is classified as ``'select'``
        so that the result set is parsed and returned.

        Returns:
            One of ``'select'``, ``'insert'``, ``'update'``, ``'delete'``,
            or ``'ddl'``.
        """
        if _RETURNING_RE.search(sql):
            return "select"
        if _SELECT_RE.match(sql):
            return "select"
        if _INSERT_RE.match(sql):
            return "insert"
        if _UPDATE_RE.match(sql):
            return "update"
        if _DELETE_RE.match(sql):
            return "delete"
        if _DDL_RE.match(sql):
            return "ddl"
        return "select"

    @staticmethod
    def execute(database, sql):
        """Route *sql* to the appropriate ``DatabaseEngine`` method.

        Args:
            database: An ``ai_server.DatabaseEngine`` instance.
            sql: The SQL string to execute.

        Returns:
            The raw result from the ``DatabaseEngine`` method.
        """
        kind = SqlRouter.classify(sql)

        if kind in ("select", "ddl"):
            return database.execQuery(query=sql)
        elif kind == "insert":
            return database.insertData(query=sql)
        elif kind == "update":
            return database.updateData(query=sql)
        elif kind == "delete":
            return database.removeData(query=sql)
        else:
            return database.execQuery(query=sql)
