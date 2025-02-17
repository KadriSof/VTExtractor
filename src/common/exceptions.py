# Custom exceptions
class BlobStorageException(Exception):
    """Base exception for Azure Blob Storage operations."""


class TransferError(Exception):
    """Exception raised during the file transfer operation."""
