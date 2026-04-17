"""SQLAlchemy dialect for SEMOSS databases.

Extends PostgreSQL's ``PGDialect`` so that SQLAlchemy generates
Postgres-compatible SQL, but routes all execution through the SEMOSS
DB-API layer.

URL format::

    semoss://engine_id
    semoss://engine_id?insight_id=xyz
"""

try:
    from sqlalchemy.dialects.postgresql import PGDialect
except ImportError:
    from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.pool import StaticPool

from . import dbapi as semoss_dbapi


class SemossDialect(PGDialect):
    """SQLAlchemy dialect backed by a SEMOSS ``DatabaseEngine``.

    Inherits PostgreSQL SQL compilation, type mapping, and identifier
    quoting. Overrides connection and transaction handling for SEMOSS's
    RPC-based, auto-commit semantics.
    """

    name = "semoss"
    driver = "semoss_dbapi"

    supports_alter = True
    supports_sequences = False
    supports_native_boolean = True
    supports_statement_cache = True
    supports_server_side_cursors = False
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    postfetch_lastrowid = False
    implicit_returning = True
    supports_default_values = True
    supports_empty_insert = False
    supports_multivalues_insert = True

    default_paramstyle = "format"

    @classmethod
    def import_dbapi(cls):
        return semoss_dbapi

    @classmethod
    def dbapi(cls):
        return semoss_dbapi

    def create_connect_args(self, url):
        """Extract ``engine_id`` from the URL host portion."""
        engine_id = url.host or ""
        opts = {"engine_id": engine_id}
        if url.query.get("insight_id"):
            opts["insight_id"] = url.query["insight_id"]
        return ([], opts)

    def do_execute(self, cursor, statement, parameters, context=None):
        cursor.execute(statement, parameters)

    def do_execute_no_params(self, cursor, statement, context=None):
        cursor.execute(statement)

    def do_commit(self, dbapi_connection):
        dbapi_connection.commit()

    def do_rollback(self, dbapi_connection):
        pass

    def do_begin(self, dbapi_connection):
        pass

    def do_close(self, dbapi_connection):
        dbapi_connection.close()

    def is_disconnect(self, e, connection, cursor):
        return False

    # --- Reflection (best-effort via information_schema) ---

    def get_table_names(self, connection, schema=None, **kw):
        try:
            result = connection.exec_driver_sql(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public'"
            )
            return [row[0] for row in result]
        except Exception:
            return []

    def get_columns(self, connection, table_name, schema=None, **kw):
        try:
            result = connection.exec_driver_sql(
                "SELECT column_name, data_type, is_nullable, column_default "
                "FROM information_schema.columns "
                "WHERE table_name = '%s'" % table_name.replace("'", "''")
            )
            columns = []
            for row in result:
                columns.append({
                    "name": row[0],
                    "type": self._resolve_type(row[1]),
                    "nullable": row[2] == "YES",
                    "default": row[3],
                })
            return columns
        except Exception:
            return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        try:
            result = connection.exec_driver_sql(
                "SELECT kcu.column_name "
                "FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu "
                "ON tc.constraint_name = kcu.constraint_name "
                "WHERE tc.table_name = '%s' AND tc.constraint_type = 'PRIMARY KEY'"
                % table_name.replace("'", "''")
            )
            cols = [row[0] for row in result]
            return {"constrained_columns": cols, "name": None}
        except Exception:
            return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def has_table(self, connection, table_name, schema=None, info_cache=None):
        try:
            names = self.get_table_names(connection, schema)
            return table_name in names
        except Exception:
            return False

    @staticmethod
    def _resolve_type(type_string):
        from sqlalchemy import types as sa_types
        mapping = {
            "integer": sa_types.INTEGER(),
            "bigint": sa_types.BIGINT(),
            "smallint": sa_types.SMALLINT(),
            "text": sa_types.TEXT(),
            "character varying": sa_types.VARCHAR(),
            "boolean": sa_types.BOOLEAN(),
            "timestamp without time zone": sa_types.TIMESTAMP(),
            "timestamp with time zone": sa_types.TIMESTAMP(timezone=True),
            "date": sa_types.DATE(),
            "double precision": sa_types.FLOAT(),
            "numeric": sa_types.NUMERIC(),
            "json": sa_types.JSON(),
            "jsonb": sa_types.JSON(),
            "real": sa_types.FLOAT(),
        }
        return mapping.get(type_string, sa_types.TEXT())
