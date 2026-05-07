"""Tests for the DB-API 2.0 layer."""

from sqlalchemy_semoss.dbapi import (
    connect,
    SemossConnection,
    SemossCursor,
    _parse_column_order,
)


class TestConnect:
    def test_returns_connection(self, mock_ai_server):
        conn = connect(engine_id="test-id")
        assert isinstance(conn, SemossConnection)

    def test_connection_creates_cursor(self, mock_ai_server):
        conn = connect(engine_id="test-id")
        cursor = conn.cursor()
        assert isinstance(cursor, SemossCursor)


class TestCursor:
    def test_execute_select(self, mock_ai_server):
        conn = connect(engine_id="test-id")
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        assert cursor.description is not None
        assert cursor.description[0][0] == "test"
        row = cursor.fetchone()
        assert row == (1,)

    def test_fetchone_returns_none_when_empty(self, mock_ai_server):
        conn = connect(engine_id="test-id")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM nonexistent")
        assert cursor.fetchone() is None

    def test_fetchall_returns_list(self, mock_ai_server):
        conn = connect(engine_id="test-id")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM nonexistent")
        assert cursor.fetchall() == []

    def test_ddl_does_not_raise(self, mock_ai_server):
        conn = connect(engine_id="test-id")
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS t (id INT)")
        assert cursor.description == ()

    def test_context_manager(self, mock_ai_server):
        with connect(engine_id="test-id") as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")


class TestEscape:
    def test_none(self):
        assert SemossCursor._escape(None) == "NULL"

    def test_bool(self):
        assert SemossCursor._escape(True) == "TRUE"
        assert SemossCursor._escape(False) == "FALSE"

    def test_int(self):
        assert SemossCursor._escape(42) == "42"

    def test_float(self):
        assert SemossCursor._escape(3.14) == "3.14"

    def test_string(self):
        assert SemossCursor._escape("hello") == "'hello'"

    def test_string_with_quotes(self):
        assert SemossCursor._escape("it's") == "'it''s'"

    def test_bytes(self):
        result = SemossCursor._escape(b"\x00\xff")
        assert "00ff" in result


class TestParseColumnOrder:
    """Unit tests for the SQL column-order parser."""

    def test_simple_select(self):
        assert _parse_column_order("SELECT name, id FROM users") == ["name", "id"]

    def test_table_qualified(self):
        assert _parse_column_order("SELECT t.name, t.id FROM t") == ["name", "id"]

    def test_aliases(self):
        assert _parse_column_order("SELECT a AS x, b AS y FROM t") == ["x", "y"]

    def test_select_star_returns_none(self):
        assert _parse_column_order("SELECT * FROM users") is None

    def test_returning_clause(self):
        sql = "INSERT INTO t (a) VALUES (1) RETURNING id, name"
        assert _parse_column_order(sql) == ["id", "name"]

    def test_returning_star_returns_none(self):
        sql = "INSERT INTO t (a) VALUES (1) RETURNING *"
        assert _parse_column_order(sql) is None

    def test_function_with_alias(self):
        assert _parse_column_order("SELECT count(*) AS cnt FROM t") == ["cnt"]

    def test_function_without_alias_returns_none(self):
        assert _parse_column_order("SELECT count(*) FROM t") is None

    def test_distinct(self):
        assert _parse_column_order("SELECT DISTINCT name, id FROM t") == ["name", "id"]

    def test_mixed_alias_and_plain(self):
        sql = "SELECT name, count(*) AS total FROM t GROUP BY name"
        assert _parse_column_order(sql) == ["name", "total"]

    def test_parenthesis_aware_split(self):
        sql = "SELECT func(a, b) AS f, name FROM t"
        assert _parse_column_order(sql) == ["f", "name"]


class TestColumnOrdering:
    """Integration tests: row positions match SQL column order, not dict key order."""

    def test_returning_reorders_to_sql_order(self, mock_ai_server):
        """row[0] should match the first column in the RETURNING clause."""
        conn = connect(engine_id="test-id")
        cursor = conn.cursor()
        # Mock returns {"id": 1, "name": "mock", "email": "mock@test.com"}
        # but RETURNING says name first, then id
        cursor.execute("INSERT INTO t (name) VALUES ('x') RETURNING name, id")
        row = cursor.fetchone()
        assert row[0] == "mock"
        assert row[1] == 1

    def test_description_matches_sql_order(self, mock_ai_server):
        """cursor.description should reflect the SQL-specified column order."""
        conn = connect(engine_id="test-id")
        cursor = conn.cursor()
        cursor.execute("INSERT INTO t (name) VALUES ('x') RETURNING name, id")
        assert cursor.description[0][0] == "name"
        assert cursor.description[1][0] == "id"

    def test_star_falls_back_without_error(self, mock_ai_server):
        """RETURNING * should fall back to dict key order gracefully."""
        conn = connect(engine_id="test-id")
        cursor = conn.cursor()
        cursor.execute("INSERT INTO t (name) VALUES ('x') RETURNING *")
        row = cursor.fetchone()
        assert row is not None
        assert len(row) == 3
