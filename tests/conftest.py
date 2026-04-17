"""Shared test fixtures — mock ai_server.DatabaseEngine."""

import sys
import types

import pytest


class MockDatabaseEngine:
    """Simulates ai_server.DatabaseEngine for testing."""

    def __init__(self, engine_id, insight_id=None):
        self.engine_id = engine_id
        self.insight_id = insight_id
        self._tables = {}  # table_name -> list of row dicts
        self._id_counters = {}  # table_name -> next id

    def execQuery(self, query):
        q = query.strip().upper()
        if q.startswith("CREATE") or q.startswith("ALTER") or q.startswith("DROP"):
            raise Exception("No results were returned by the query.")
        if q.startswith("SELECT"):
            # Handle "SELECT 1 as test" style literal queries
            if "FROM" not in q:
                return [{"test": 1}]
            # Return canned data based on table
            for table_name, rows in self._tables.items():
                if table_name.upper() in q:
                    if "COUNT(*)" in q:
                        return [{"count": len(rows)}]
                    if "ORDER BY" in q and "DESC" in q and "LIMIT 1" in q:
                        return [rows[-1]] if rows else []
                    return list(rows)
            return []
        raise Exception("No results were returned by the query.")

    def insertData(self, query):
        return None

    def updateData(self, query):
        return None

    def removeData(self, query):
        return None


@pytest.fixture
def mock_ai_server():
    """Install a mock ai_server module and return the mock DatabaseEngine class."""
    ai_server = types.ModuleType("ai_server")
    ai_server.DatabaseEngine = MockDatabaseEngine
    sys.modules["ai_server"] = ai_server
    yield MockDatabaseEngine
    del sys.modules["ai_server"]
