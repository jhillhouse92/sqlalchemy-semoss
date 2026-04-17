"""Tests for SQL classification and routing."""

from sqlalchemy_semoss.router import SqlRouter


class TestClassify:
    def test_select(self):
        assert SqlRouter.classify("SELECT * FROM users") == "select"

    def test_select_with_whitespace(self):
        assert SqlRouter.classify("  SELECT id FROM t") == "select"

    def test_with_cte(self):
        assert SqlRouter.classify("WITH cte AS (SELECT 1) SELECT * FROM cte") == "select"

    def test_insert(self):
        assert SqlRouter.classify("INSERT INTO users (name) VALUES ('a')") == "insert"

    def test_update(self):
        assert SqlRouter.classify("UPDATE users SET name = 'b' WHERE id = 1") == "update"

    def test_delete(self):
        assert SqlRouter.classify("DELETE FROM users WHERE id = 1") == "delete"

    def test_truncate(self):
        assert SqlRouter.classify("TRUNCATE TABLE users") == "delete"

    def test_create_table(self):
        assert SqlRouter.classify("CREATE TABLE t (id INT)") == "ddl"

    def test_alter(self):
        assert SqlRouter.classify("ALTER TABLE t ADD COLUMN x INT") == "ddl"

    def test_drop(self):
        assert SqlRouter.classify("DROP TABLE t") == "ddl"

    def test_unknown_defaults_to_select(self):
        assert SqlRouter.classify("EXPLAIN ANALYZE SELECT 1") == "select"

    def test_case_insensitive(self):
        assert SqlRouter.classify("select * from t") == "select"
        assert SqlRouter.classify("INSERT into t values (1)") == "insert"


class TestExecute:
    def test_select_routes_to_exec_query(self, mock_ai_server):
        db = mock_ai_server(engine_id="test")
        result = SqlRouter.execute(db, "SELECT 1")
        assert isinstance(result, list)

    def test_insert_routes_to_insert_data(self, mock_ai_server):
        db = mock_ai_server(engine_id="test")
        result = SqlRouter.execute(db, "INSERT INTO t (a) VALUES (1)")
        assert result is None

    def test_ddl_routes_to_exec_query(self, mock_ai_server):
        db = mock_ai_server(engine_id="test")
        # DDL raises "No results" which is expected
        try:
            SqlRouter.execute(db, "CREATE TABLE t (id INT)")
        except Exception as e:
            assert "No results" in str(e)
