"""Tests for the DB-API 2.0 layer."""

import datetime

from sqlalchemy_semoss.dbapi import (
    connect,
    SemossConnection,
    SemossCursor,
    _parse_column_order,
    _coerce_value,
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


class TestCoerceValue:
    """Unit tests for _coerce_value type inference."""

    def test_none_passthrough(self):
        assert _coerce_value(None) is None

    def test_non_string_passthrough(self):
        assert _coerce_value(42) == 42
        assert _coerce_value(3.14) == 3.14
        assert _coerce_value(True) is True

    def test_bool_true(self):
        assert _coerce_value("true") is True
        assert _coerce_value("True") is True
        assert _coerce_value("TRUE") is True

    def test_bool_false(self):
        assert _coerce_value("false") is False
        assert _coerce_value("False") is False
        assert _coerce_value("FALSE") is False

    def test_integer(self):
        assert _coerce_value("1") == 1
        assert _coerce_value("42") == 42
        assert _coerce_value("-7") == -7
        assert _coerce_value("0") == 0

    def test_float(self):
        assert _coerce_value("19.99") == 19.99
        assert _coerce_value("3.14") == 3.14
        assert _coerce_value("-0.5") == -0.5
        assert _coerce_value("95.5") == 95.5

    def test_float_scientific(self):
        assert _coerce_value("1e10") == 1e10
        assert _coerce_value("2.5E-3") == 2.5e-3

    def test_date(self):
        assert _coerce_value("1990-03-25") == datetime.date(1990, 3, 25)
        assert _coerce_value("2000-12-31") == datetime.date(2000, 12, 31)

    def test_timestamp(self):
        assert _coerce_value("2024-06-15 10:30:00") == datetime.datetime(2024, 6, 15, 10, 30, 0)
        assert _coerce_value("2025-01-01 00:00:00") == datetime.datetime(2025, 1, 1, 0, 0, 0)

    def test_plain_string_unchanged(self):
        assert _coerce_value("Alice") == "Alice"
        assert _coerce_value("hello world") == "hello world"
        assert _coerce_value("") == ""

    def test_string_not_matching_patterns(self):
        assert _coerce_value("not-a-date") == "not-a-date"
        assert _coerce_value("12abc") == "12abc"


class TestTypeCoercionIntegration:
    """Integration: string values from SEMOSS get coerced to native types."""

    def test_list_of_dicts_values_coerced(self, mock_ai_server):
        """Simulate SEMOSS response as list-of-dicts with all-string values."""
        conn = connect(engine_id="test-id")
        cursor = conn.cursor()

        # Simulate what the real server returns (all strings)
        raw = [
            {
                "id": "1",
                "name": "Alice",
                "price": "19.99",
                "active": "true",
                "created_at": "2024-06-15 10:30:00",
                "birth_date": "1990-03-25",
            }
        ]

        rows, columns = cursor._parse_select_result(raw)

        assert len(rows) == 1
        row = rows[0]

        assert row[columns.index("id")] == 1
        assert isinstance(row[columns.index("id")], int)

        assert row[columns.index("name")] == "Alice"
        assert isinstance(row[columns.index("name")], str)

        assert row[columns.index("price")] == 19.99
        assert isinstance(row[columns.index("price")], float)

        assert row[columns.index("active")] is True
        assert isinstance(row[columns.index("active")], bool)

        assert row[columns.index("created_at")] == datetime.datetime(2024, 6, 15, 10, 30, 0)

        assert row[columns.index("birth_date")] == datetime.date(1990, 3, 25)

    def test_false_bool_is_falsy(self, mock_ai_server):
        """Critical: 'false' must become Python False, not truthy string."""
        conn = connect(engine_id="test-id")
        cursor = conn.cursor()

        raw = [{"active": "false"}]
        rows, columns = cursor._parse_select_result(raw)

        assert rows[0][0] is False
        assert not rows[0][0]  # Must be falsy!

    def test_none_preserved(self, mock_ai_server):
        """None values should stay None, not get coerced."""
        conn = connect(engine_id="test-id")
        cursor = conn.cursor()

        raw = [{"id": "1", "name": None}]
        rows, columns = cursor._parse_select_result(raw)

        assert rows[0][columns.index("id")] == 1
        assert rows[0][columns.index("name")] is None
