"""PEP 249 DB-API 2.0 exception hierarchy.

These exceptions follow the standard defined in :pep:`249` and can be
caught by code written against any DB-API 2.0 driver.
"""


class Warning(Exception):
    """Exception raised for important warnings."""


class Error(Exception):
    """Base class for all DB-API error exceptions."""


class InterfaceError(Error):
    """Exception raised for errors related to the database interface."""


class DatabaseError(Error):
    """Exception raised for errors related to the database."""


class DataError(DatabaseError):
    """Exception raised for errors due to problems with processed data."""


class OperationalError(DatabaseError):
    """Exception raised for errors related to the database's operation."""


class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the database is affected."""


class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal error."""


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors (bad SQL, wrong parameters, etc.)."""


class NotSupportedError(DatabaseError):
    """Exception raised when a method or database API is not supported."""
