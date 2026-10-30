class WriterError(Exception):
    """Safe diagnostic suitable for the CLI; never includes subprocess stderr."""


class AuthenticationError(WriterError):
    pass


class StorageError(WriterError):
    pass


class CatalogError(WriterError):
    pass


class Conflict(WriterError):
    pass


class PublicationUnknown(WriterError):
    """Publication might have succeeded. Keep the files and retry lookup."""
