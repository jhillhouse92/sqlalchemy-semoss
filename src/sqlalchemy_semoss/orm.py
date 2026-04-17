"""Active Record ORM for SEMOSS databases.

Provides :class:`SemossModel` — a SQLAlchemy ``DeclarativeBase`` with
Rails-style convenience methods (``save``, ``delete``, ``get``, ``find``,
``all``, ``where``).

All database operations use raw SQL via the DB-API layer. SQLAlchemy is
used only for model/table metadata (column definitions).

Usage::

    from sqlalchemy_semoss import configure, SemossModel
    from sqlalchemy import Column, Integer, String

    configure("your-engine-uuid")

    class User(SemossModel):
        __tablename__ = "users"
        id = Column(Integer, primary_key=True)
        name = Column(String(100))

    user = User(name="Alice")
    user.save()       # INSERT ... RETURNING *
    user.id           # auto-populated
    User.get(1)       # SELECT by PK
    User.all()        # SELECT *
    User.find(name="Alice")
    User.where(name="Alice").order_by("-id").limit(10).all()
"""

from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy import inspect as sa_inspect

# ---------------------------------------------------------------------------
# Module-level state — set by configure()
# ---------------------------------------------------------------------------

_engine_id = None
_sa_engine = None
_SessionFactory = None


def configure(engine_id, **engine_kwargs):
    """Initialise the ORM for a specific SEMOSS database engine.

    Must be called once before using any :class:`SemossModel` methods.

    Args:
        engine_id: The SEMOSS database engine UUID.
        **engine_kwargs: Extra arguments forwarded to
            :func:`sqlalchemy_semoss.create_engine` (e.g. ``echo=True``).
    """
    global _engine_id, _sa_engine, _SessionFactory

    _engine_id = engine_id

    from .engine import create_engine
    _sa_engine = create_engine(engine_id, **engine_kwargs)
    _SessionFactory = sessionmaker(bind=_sa_engine)


def get_session():
    """Return a new SQLAlchemy ``Session`` from the configured factory.

    Raises:
        RuntimeError: If :func:`configure` has not been called.
    """
    if _SessionFactory is None:
        raise RuntimeError(
            "sqlalchemy_semoss is not configured. "
            "Call sqlalchemy_semoss.configure(engine_id) first."
        )
    return _SessionFactory()


def get_engine():
    """Return the configured SQLAlchemy ``Engine``.

    Raises:
        RuntimeError: If :func:`configure` has not been called.
    """
    if _sa_engine is None:
        raise RuntimeError(
            "sqlalchemy_semoss is not configured. "
            "Call sqlalchemy_semoss.configure(engine_id) first."
        )
    return _sa_engine


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_conn():
    """Get a DB-API connection using the configured engine ID."""
    if _engine_id is None:
        raise RuntimeError(
            "sqlalchemy_semoss is not configured. "
            "Call sqlalchemy_semoss.configure(engine_id) first."
        )
    from .dbapi import connect
    return connect(engine_id=_engine_id)


def _escape(value):
    """Escape a Python value for safe SQL interpolation."""
    from .dbapi import SemossCursor
    return SemossCursor._escape(value)


# ---------------------------------------------------------------------------
# Declarative base
# ---------------------------------------------------------------------------

class SemossBase(DeclarativeBase):
    """SQLAlchemy declarative base for SEMOSS models."""
    pass


# ---------------------------------------------------------------------------
# Active Record mixin
# ---------------------------------------------------------------------------

class ActiveRecordMixin:
    """Mixin providing Active Record-style instance and class methods.

    All operations go directly through the SEMOSS DB-API layer.
    """

    def save(self):
        """INSERT or UPDATE this instance.

        For new records (``pk is None``), executes
        ``INSERT ... RETURNING *`` and populates the instance with the
        returned values (including auto-generated primary keys).

        Returns:
            ``self``, for chaining.
        """
        table = self.__class__.__table__
        mapper = sa_inspect(self.__class__)
        pk_cols = [c.name for c in mapper.primary_key]

        data = {}
        for col in table.columns:
            val = getattr(self, col.name, None)
            if col.name in pk_cols and val is None:
                continue
            data[col.name] = val

        pk_val = getattr(self, pk_cols[0], None) if len(pk_cols) == 1 else None
        is_new = pk_val is None

        conn = _get_conn()
        cursor = conn.cursor()
        try:
            if is_new:
                cols = ", ".join(data.keys())
                vals = ", ".join(_escape(v) for v in data.values())
                sql = "INSERT INTO %s (%s) VALUES (%s) RETURNING *" % (
                    table.name, cols, vals)
                cursor.execute(sql)
                row = cursor.fetchone()
                if row and cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    for col_name, val in zip(columns, row):
                        setattr(self, col_name, val)
            else:
                set_parts = []
                for k, v in data.items():
                    if k not in pk_cols:
                        set_parts.append("%s = %s" % (k, _escape(v)))
                where_parts = [
                    "%s = %s" % (pk, _escape(getattr(self, pk)))
                    for pk in pk_cols
                ]
                sql = "UPDATE %s SET %s WHERE %s" % (
                    table.name, ", ".join(set_parts), " AND ".join(where_parts))
                cursor.execute(sql)
        finally:
            cursor.close()
            conn.close()

        return self

    def delete(self):
        """DELETE this instance from the database."""
        table = self.__class__.__table__
        mapper = sa_inspect(self.__class__)
        pk_cols = [c.name for c in mapper.primary_key]

        conn = _get_conn()
        cursor = conn.cursor()
        try:
            where_parts = [
                "%s = %s" % (pk, _escape(getattr(self, pk)))
                for pk in pk_cols
            ]
            sql = "DELETE FROM %s WHERE %s" % (
                table.name, " AND ".join(where_parts))
            cursor.execute(sql)
        finally:
            cursor.close()
            conn.close()

    @classmethod
    def get(cls, pk):
        """Find a record by primary key.

        Returns:
            A model instance, or ``None`` if not found.
        """
        table = cls.__table__
        mapper = sa_inspect(cls)
        pk_col = mapper.primary_key[0].name

        conn = _get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM %s WHERE %s = %s"
                % (table.name, pk_col, _escape(pk))
            )
            row = cursor.fetchone()
            if row is None:
                return None
            columns = [desc[0] for desc in cursor.description]
            return cls(**dict(zip(columns, row)))
        finally:
            cursor.close()
            conn.close()

    @classmethod
    def find(cls, **kwargs):
        """Find all records matching column conditions (AND).

        Returns:
            A list of model instances.
        """
        table = cls.__table__

        conn = _get_conn()
        cursor = conn.cursor()
        try:
            sql = "SELECT * FROM %s" % table.name
            if kwargs:
                where_parts = [
                    "%s = %s" % (k, _escape(v)) for k, v in kwargs.items()
                ]
                sql += " WHERE " + " AND ".join(where_parts)
            cursor.execute(sql)
            if not cursor.description:
                return []
            columns = [desc[0] for desc in cursor.description]
            return [cls(**dict(zip(columns, row))) for row in cursor.fetchall()]
        finally:
            cursor.close()
            conn.close()

    @classmethod
    def all(cls):
        """Return all records of this model type."""
        table = cls.__table__

        conn = _get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM %s" % table.name)
            if not cursor.description:
                return []
            columns = [desc[0] for desc in cursor.description]
            return [cls(**dict(zip(columns, row))) for row in cursor.fetchall()]
        finally:
            cursor.close()
            conn.close()

    @classmethod
    def count(cls, **kwargs):
        """Count records matching optional conditions."""
        table = cls.__table__

        conn = _get_conn()
        cursor = conn.cursor()
        try:
            sql = "SELECT COUNT(*) FROM %s" % table.name
            if kwargs:
                where_parts = [
                    "%s = %s" % (k, _escape(v)) for k, v in kwargs.items()
                ]
                sql += " WHERE " + " AND ".join(where_parts)
            cursor.execute(sql)
            row = cursor.fetchone()
            return row[0] if row else 0
        finally:
            cursor.close()
            conn.close()

    @classmethod
    def where(cls, **kwargs):
        """Begin a chainable query.

        Returns:
            A :class:`QueryBuilder` instance.
        """
        return QueryBuilder(cls).where(**kwargs)


# ---------------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------------

class QueryBuilder:
    """Chainable query builder using raw SQL via the DB-API layer.

    Example::

        User.where(active=True).order_by("-created_at").limit(10).all()
    """

    def __init__(self, model_class):
        self._model = model_class
        self._wheres = {}
        self._order = []
        self._limit_val = None
        self._offset_val = None

    def where(self, **kwargs):
        """Add equality filter conditions (AND)."""
        self._wheres.update(kwargs)
        return self

    def order_by(self, *columns):
        """Order results. Prefix a column name with ``-`` for descending."""
        for col in columns:
            if isinstance(col, str):
                if col.startswith("-"):
                    self._order.append("%s DESC" % col[1:])
                else:
                    self._order.append("%s ASC" % col)
        return self

    def limit(self, n):
        """Limit the number of results."""
        self._limit_val = n
        return self

    def offset(self, n):
        """Skip the first *n* results."""
        self._offset_val = n
        return self

    def _build_sql(self, select_expr="*"):
        table = self._model.__table__
        sql = "SELECT %s FROM %s" % (select_expr, table.name)
        if self._wheres:
            parts = [
                "%s = %s" % (k, _escape(v)) for k, v in self._wheres.items()
            ]
            sql += " WHERE " + " AND ".join(parts)
        if self._order:
            sql += " ORDER BY " + ", ".join(self._order)
        if self._limit_val is not None:
            sql += " LIMIT %d" % self._limit_val
        if self._offset_val is not None:
            sql += " OFFSET %d" % self._offset_val
        return sql

    def all(self):
        """Execute and return all matching records."""
        conn = _get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(self._build_sql())
            if not cursor.description:
                return []
            columns = [desc[0] for desc in cursor.description]
            return [
                self._model(**dict(zip(columns, row)))
                for row in cursor.fetchall()
            ]
        finally:
            cursor.close()
            conn.close()

    def first(self):
        """Execute and return the first matching record, or ``None``."""
        self._limit_val = 1
        results = self.all()
        return results[0] if results else None

    def count(self):
        """Return the count of matching records."""
        conn = _get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(self._build_sql(select_expr="COUNT(*)"))
            row = cursor.fetchone()
            return row[0] if row else 0
        finally:
            cursor.close()
            conn.close()


# ---------------------------------------------------------------------------
# Model base class
# ---------------------------------------------------------------------------

class SemossModel(SemossBase, ActiveRecordMixin):
    """Base class for SEMOSS ORM models.

    Combines SQLAlchemy's ``DeclarativeBase`` (for column/table metadata)
    with Active Record methods that use raw SQL via the DB-API layer.

    After ``save()``, auto-generated fields (id, defaults) are populated
    via ``RETURNING``.
    """
    __abstract__ = True
