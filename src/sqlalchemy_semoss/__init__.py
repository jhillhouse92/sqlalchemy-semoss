"""SQLAlchemy dialect and Active Record ORM for SEMOSS databases.

``sqlalchemy-semoss`` makes SEMOSS's ``ai_server.DatabaseEngine`` accessible
through standard Python database interfaces:

- **PEP 249 DB-API 2.0** — ``connect()``, ``Cursor``, ``Connection``
- **SQLAlchemy dialect** — ``create_engine("semoss://engine_id")``
- **Active Record ORM** — ``SemossModel`` with ``save()``, ``get()``,
  ``find()``, ``all()``, ``where()``

Quick start::

    from sqlalchemy_semoss import configure, SemossModel
    from sqlalchemy import Column, Integer, String

    configure("your-engine-uuid")

    class User(SemossModel):
        __tablename__ = "users"
        id = Column(Integer, primary_key=True)
        name = Column(String(100))

    User(name="Alice").save()
    User.get(1)
    User.find(name="Alice")
"""

__version__ = "0.1.0"

# DB-API 2.0
from .dbapi import connect, SemossConnection, SemossCursor

# Exceptions
from .exceptions import (
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
)

# Type objects
from .types import STRING, BINARY, NUMBER, DATETIME, ROWID

# SQLAlchemy dialect
from .dialect import SemossDialect
from .engine import create_engine

# ORM
from .orm import (
    configure,
    get_session,
    get_engine,
    SemossBase,
    SemossModel,
    QueryBuilder,
)

# PEP 249 module-level attributes
apilevel = "2.0"
threadsafety = 1
paramstyle = "format"

__all__ = [
    # DB-API
    "connect",
    "SemossConnection",
    "SemossCursor",
    # Exceptions
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    # Types
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    # Dialect / Engine
    "SemossDialect",
    "create_engine",
    # ORM
    "configure",
    "get_session",
    "get_engine",
    "SemossBase",
    "SemossModel",
    "QueryBuilder",
]
