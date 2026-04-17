# sqlalchemy-semoss

[![PyPI version](https://img.shields.io/pypi/v/sqlalchemy-semoss.svg)](https://pypi.org/project/sqlalchemy-semoss/)
[![Python](https://img.shields.io/pypi/pyversions/sqlalchemy-semoss.svg)](https://pypi.org/project/sqlalchemy-semoss/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**SQLAlchemy dialect and Active Record ORM for [SEMOSS](https://semoss.org) databases.**

Makes SEMOSS's `ai_server.DatabaseEngine` accessible through standard Python database interfaces — PEP 249 DB-API 2.0, SQLAlchemy dialect, and an Active Record ORM with `save()`, `get()`, `find()`, `all()`, and `where()`.

## Architecture

```
Your Application
       │
   sqlalchemy_semoss.orm        Active Record (save/delete/get/find/where)
       │
   sqlalchemy_semoss.dialect    SQLAlchemy Dialect (extends PGDialect)
       │
   sqlalchemy_semoss.dbapi      PEP 249 DB-API 2.0 (Connection, Cursor)
       │
   ai_server.DatabaseEngine     SEMOSS runtime (execQuery, insertData, ...)
       │
   PostgreSQL / RDBMS           Actual database
```

## Installation

```bash
pip install sqlalchemy-semoss
```

> **Note:** The `ai_server` module is provided by the SEMOSS runtime and does not need to be installed separately when running inside SEMOSS. For external development/testing, install `pip install ai-server-sdk`.

## Quick Start

### 1. Raw DB-API

Use the PEP 249 interface directly for full control:

```python
from sqlalchemy_semoss import connect

conn = connect(engine_id="your-engine-uuid")
cursor = conn.cursor()

cursor.execute("SELECT * FROM users WHERE active = %s", (True,))
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
```

### 2. Active Record ORM

Define models with SQLAlchemy columns, then use Rails-style methods:

```python
from sqlalchemy_semoss import configure, SemossModel
from sqlalchemy import Column, Integer, String, DateTime

# Call once at startup
configure("your-engine-uuid")

class User(SemossModel):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(255))

# Create
user = User(name="Alice", email="alice@example.com")
user.save()
print(user.id)  # auto-populated via RETURNING

# Read
user = User.get(1)                          # by primary key
users = User.find(name="Alice")             # by conditions
all_users = User.all()                      # all records
count = User.count(name="Alice")            # count

# Query builder
users = (User.where(active=True)
             .order_by("-created_at")
             .limit(10)
             .all())

first = User.where(name="Alice").first()

# Update
user.name = "Bob"
user.save()

# Delete
user.delete()
```

### 3. SQLAlchemy Engine

Use the dialect directly with SQLAlchemy's `create_engine`:

```python
from sqlalchemy_semoss import create_engine

engine = create_engine("your-engine-uuid")

# Or via URL string (works after pip install due to entry-point registration):
from sqlalchemy import create_engine as sa_create_engine
engine = sa_create_engine("semoss://your-engine-uuid")
```

## Building Framework Adapters

The DB-API layer (`connect`, `SemossCursor`) provides everything needed to build integrations with frameworks like APScheduler, LangGraph, Celery, etc.

```python
from sqlalchemy_semoss import connect

conn = connect(engine_id="your-engine-uuid")
cursor = conn.cursor()

# Execute any SQL
cursor.execute("CREATE TABLE IF NOT EXISTS jobs (id VARCHAR PRIMARY KEY, state TEXT)")
cursor.execute("INSERT INTO jobs (id, state) VALUES (%s, %s)", ("job-1", "pending"))
cursor.execute("SELECT * FROM jobs WHERE id = %s", ("job-1",))
row = cursor.fetchone()
```

See the [SemossDB example app](https://github.com/jhillhouse92/semoss_mcp_example) for working APScheduler and LangGraph adapter implementations.

## API Reference

### Module-Level Functions

| Function | Description |
|----------|-------------|
| `connect(engine_id, insight_id=None)` | Create a DB-API 2.0 connection |
| `configure(engine_id, **kwargs)` | Initialize the ORM (call once at startup) |
| `create_engine(engine_id, **kwargs)` | Create a SQLAlchemy `Engine` |
| `get_session()` | Get a new SQLAlchemy `Session` |
| `get_engine()` | Get the configured SQLAlchemy `Engine` |

### Classes

| Class | Description |
|-------|-------------|
| `SemossConnection` | DB-API 2.0 Connection (wraps `DatabaseEngine`) |
| `SemossCursor` | DB-API 2.0 Cursor (execute, fetch, iterate) |
| `SemossDialect` | SQLAlchemy dialect (extends `PGDialect`) |
| `SemossModel` | Active Record base class (extend for your models) |
| `SemossBase` | SQLAlchemy `DeclarativeBase` (for advanced use) |
| `QueryBuilder` | Chainable query builder (returned by `Model.where()`) |

### Active Record Methods

**Instance methods:**

| Method | Description |
|--------|-------------|
| `save()` | INSERT (new) or UPDATE (existing). Returns `self`. |
| `delete()` | DELETE this record. |

**Class methods:**

| Method | Description |
|--------|-------------|
| `Model.get(pk)` | Find by primary key. Returns instance or `None`. |
| `Model.find(**kwargs)` | Find all matching conditions (AND). |
| `Model.all()` | Return all records. |
| `Model.count(**kwargs)` | Count matching records. |
| `Model.where(**kwargs)` | Start a chainable `QueryBuilder`. |

**QueryBuilder methods:**

| Method | Description |
|--------|-------------|
| `.where(**kwargs)` | Add filter conditions (AND). |
| `.order_by(*cols)` | Order results. Prefix with `-` for DESC. |
| `.limit(n)` | Limit results. |
| `.offset(n)` | Skip first *n* results. |
| `.all()` | Execute and return all matches. |
| `.first()` | Execute and return first match or `None`. |
| `.count()` | Execute and return count. |

## How It Works

### SQL Routing

The SEMOSS `DatabaseEngine` exposes four methods. The driver classifies each SQL statement and routes it accordingly:

| SQL Type | Method Called |
|----------|-------------|
| `SELECT`, `WITH`, `SHOW`, `EXPLAIN` | `database.execQuery(query=sql)` |
| `INSERT` | `database.insertData(query=sql)` |
| `UPDATE` | `database.updateData(query=sql)` |
| `DELETE`, `TRUNCATE` | `database.removeData(query=sql)` |
| `CREATE`, `ALTER`, `DROP` | `database.execQuery(query=sql)` |

**INSERT ... RETURNING:** DML statements with a `RETURNING` clause are routed through `execQuery` (not `insertData`) since they return a result set. This is how `save()` populates auto-generated fields like primary keys.

### Parameter Interpolation

Since `DatabaseEngine` methods accept only final SQL strings, parameters are interpolated before execution. The cursor handles escaping of strings, numbers, dates, booleans, bytes, and `None` (→ `NULL`).

### Auto-Commit

SEMOSS database operations auto-commit individually. `commit()` and `rollback()` are no-ops.

## Limitations

- **No transactions** — each operation auto-commits; rollback is not supported
- **No SQLAlchemy session queries** — `session.execute(select(Model))` is not supported; use Active Record methods instead
- **No relationships** — `relationship()` and `ForeignKey` joins are not supported through Active Record
- **No server-side cursors** — results are fetched eagerly in one RPC call
- **Schema reflection** — best-effort via `information_schema`; may not work on all SEMOSS backends

## Contributing

Contributions are welcome! Please open an issue or pull request on [GitHub](https://github.com/jhillhouse92/sqlalchemy-semoss).

## License

[MIT](LICENSE)
