"""Tests for the Active Record ORM layer."""

import pytest
from sqlalchemy import Column, Integer, String

from sqlalchemy_semoss.orm import configure, SemossModel, QueryBuilder


class User(SemossModel):
    __tablename__ = "test_users"
    __table_args__ = {"extend_existing": True}
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(255))


class TestConfigure:
    def test_configure_sets_engine_id(self, mock_ai_server):
        from sqlalchemy_semoss import orm
        configure("test-engine-id")
        assert orm._engine_id == "test-engine-id"

    def test_configure_creates_engine(self, mock_ai_server):
        from sqlalchemy_semoss import orm
        configure("test-engine-id")
        assert orm._sa_engine is not None


class TestActiveRecord:
    @pytest.fixture(autouse=True)
    def setup(self, mock_ai_server):
        configure("test-engine-id")

    def test_save_new_record(self):
        user = User(name="Alice", email="alice@test.com")
        result = user.save()
        assert result is user

    def test_all_returns_list(self):
        users = User.all()
        assert isinstance(users, list)

    def test_get_returns_none_for_missing(self):
        result = User.get(99999)
        assert result is None

    def test_count_returns_int(self):
        result = User.count()
        assert isinstance(result, int)


class TestQueryBuilder:
    @pytest.fixture(autouse=True)
    def setup(self, mock_ai_server):
        configure("test-engine-id")

    def test_where_returns_query_builder(self):
        qb = User.where(name="Alice")
        assert isinstance(qb, QueryBuilder)

    def test_chaining(self):
        qb = User.where(name="Alice").order_by("-id").limit(10).offset(5)
        assert qb._limit_val == 10
        assert qb._offset_val == 5

    def test_build_sql(self):
        qb = User.where(name="Alice").order_by("-id").limit(5)
        sql = qb._build_sql()
        assert "test_users" in sql
        assert "WHERE" in sql
        assert "ORDER BY" in sql
        assert "LIMIT" in sql
