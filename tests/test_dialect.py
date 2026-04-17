"""Tests for the SQLAlchemy dialect."""

from sqlalchemy_semoss.dialect import SemossDialect


class TestDialect:
    def test_name(self):
        assert SemossDialect.name == "semoss"

    def test_driver(self):
        assert SemossDialect.driver == "semoss_dbapi"

    def test_implicit_returning(self):
        assert SemossDialect.implicit_returning is True

    def test_supports_native_boolean(self):
        assert SemossDialect.supports_native_boolean is True

    def test_import_dbapi(self):
        mod = SemossDialect.import_dbapi()
        assert hasattr(mod, "connect")
        assert hasattr(mod, "SemossConnection")
        assert hasattr(mod, "SemossCursor")
