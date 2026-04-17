"""SQLAlchemy engine factory and dialect registration.

Registers the ``semoss://`` URL scheme with SQLAlchemy so that
``create_engine("semoss://engine_id")`` works out of the box.

Usage::

    from sqlalchemy_semoss import create_engine

    engine = create_engine("my-database-engine-uuid")
"""

from sqlalchemy import create_engine as sa_create_engine
from sqlalchemy.dialects import registry
from sqlalchemy.pool import StaticPool

# Register the dialect.  The entry-point in pyproject.toml also does this
# for installed packages, but explicit registration ensures it works even
# when the package is used from source without installing.
registry.register("semoss", "sqlalchemy_semoss.dialect", "SemossDialect")
registry.register("semoss.semoss_dbapi", "sqlalchemy_semoss.dialect", "SemossDialect")


def create_engine(engine_id, **kwargs):
    """Create a SQLAlchemy engine for a SEMOSS database.

    Uses ``StaticPool`` since each "connection" is a lightweight handle
    to a ``DatabaseEngine``, not a real socket.

    Args:
        engine_id: The SEMOSS database engine UUID.
        **kwargs: Additional arguments passed to
            :func:`sqlalchemy.create_engine` (e.g. ``echo=True``).

    Returns:
        A :class:`sqlalchemy.engine.Engine` instance.
    """
    return sa_create_engine(
        "semoss://%s" % engine_id,
        poolclass=StaticPool,
        pool_pre_ping=False,
        **kwargs,
    )
