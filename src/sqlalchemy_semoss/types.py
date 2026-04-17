"""PEP 249 DB-API 2.0 type objects and constructors.

Provides the standard type comparison objects (:data:`STRING`, :data:`NUMBER`,
etc.) and date/time constructors required by :pep:`249`.
"""

import datetime


class _DBAPITypeObject:
    """Compares equal to any of the given type strings (case-insensitive)."""

    def __init__(self, *values):
        self.values = frozenset(v.lower() for v in values)

    def __eq__(self, other):
        if isinstance(other, str):
            return other.lower() in self.values
        return NotImplemented

    def __ne__(self, other):
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    def __hash__(self):
        return hash(self.values)


STRING = _DBAPITypeObject(
    "str", "varchar", "text", "char", "character varying", "name", "uuid",
)
BINARY = _DBAPITypeObject("bytes", "bytea", "blob")
NUMBER = _DBAPITypeObject(
    "int", "float", "decimal", "numeric", "integer", "bigint", "smallint",
    "double precision", "real", "serial", "bigserial",
)
DATETIME = _DBAPITypeObject(
    "datetime", "timestamp", "date", "time",
    "timestamp without time zone", "timestamp with time zone",
)
ROWID = _DBAPITypeObject("oid", "rowid")

# PEP 249 constructor functions
Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
DateFromTicks = lambda ticks: datetime.date.fromtimestamp(ticks)
TimeFromTicks = lambda ticks: datetime.datetime.fromtimestamp(ticks).time()
TimestampFromTicks = lambda ticks: datetime.datetime.fromtimestamp(ticks)
Binary = bytes
