"""Tests for the DB-API 2.0 layer."""

from sqlalchemy_semoss.dbapi import connect, SemossConnection, SemossCursor


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
