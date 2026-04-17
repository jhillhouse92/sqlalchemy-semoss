"""SQL statement classification and routing.

Classifies SQL statements by type (SELECT, INSERT, UPDATE, DELETE, DDL) and
dispatches them to the correct ``ai_server.DatabaseEngine`` method.

The SEMOSS ``DatabaseEngine`` exposes four methods:

- ``execQuery(query=...)`` — for SELECT and DDL
- ``insertData(query=...)`` — for INSERT
- ``updateData(query=...)`` — for UPDATE
- ``removeData(query=...)`` — for DELETE

For ``INSERT ... RETURNING`` statements, the router strips the RETURNING
clause, executes via ``insertData``, then fetches the newly inserted row
via ``execQuery`` with a follow-up SELECT.
"""

import re

_SELECT_RE = re.compile(r"^\s*(SELECT|WITH|EXPLAIN|SHOW|DESCRIBE)\b", re.IGNORECASE)
_INSERT_RE = re.compile(r"^\s*INSERT\b", re.IGNORECASE)
_UPDATE_RE = re.compile(r"^\s*UPDATE\b", re.IGNORECASE)
_DELETE_RE = re.compile(r"^\s*(DELETE|TRUNCATE)\b", re.IGNORECASE)
_DDL_RE = re.compile(r"^\s*(CREATE|ALTER|DROP|GRANT|REVOKE)\b", re.IGNORECASE)
_RETURNING_RE = re.compile(r"\s+RETURNING\s+.+$", re.IGNORECASE)
_INSERT_TABLE_RE = re.compile(r"^\s*INSERT\s+INTO\s+(\S+)", re.IGNORECASE)


class SqlRouter:
    """Classifies SQL statements and dispatches them to the appropriate
    ``DatabaseEngine`` method.
    """

    @staticmethod
    def classify(sql):
        """Classify a SQL statement.

        Returns:
            One of ``'select'``, ``'insert'``, ``'update'``, ``'delete'``,
            or ``'ddl'``.
        """
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
            returning_match = _RETURNING_RE.search(sql)
            if returning_match:
                insert_sql = sql[:returning_match.start()]
                database.insertData(query=insert_sql)
                table_match = _INSERT_TABLE_RE.match(sql)
                if table_match:
                    table_name = table_match.group(1)
                    return database.execQuery(
                        query="SELECT * FROM %s ORDER BY id DESC LIMIT 1" % table_name
                    )
                return None
            return database.insertData(query=sql)
        elif kind == "update":
            return database.updateData(query=sql)
        elif kind == "delete":
            return database.removeData(query=sql)
        else:
            return database.execQuery(query=sql)
