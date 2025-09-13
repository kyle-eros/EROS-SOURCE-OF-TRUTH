# src/exceptions.py
"""
Custom exceptions for Gmail ETL pipeline.
Provides clear error hierarchy for different failure scenarios.
"""

class PipelineError(Exception):
    """Base exception for all pipeline errors"""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}
    
    def __str__(self):
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} [{details_str}]"
        return self.message


class AuthenticationError(PipelineError):
    """Raised when Gmail authentication fails"""
    pass


class GmailError(PipelineError):
    """Base class for Gmail-related errors"""
    pass


class NoMessagesFoundError(GmailError):
    """Raised when no messages match the search criteria"""
    pass


class MessageFetchError(GmailError):
    """Raised when unable to fetch a specific message"""
    def __init__(self, message_id: str, reason: str = None):
        details = {"message_id": message_id}
        if reason:
            details["reason"] = reason
        super().__init__(f"Failed to fetch message {message_id}", details)


class DownloadError(PipelineError):
    """Base class for download-related errors"""
    pass


class NoDownloadUrlFound(DownloadError):
    """Raised when no download URL is found in email"""
    def __init__(self, message_id: str = None, page_name: str = None):
        details = {}
        if message_id:
            details["message_id"] = message_id
        if page_name:
            details["page_name"] = page_name
        super().__init__("No download URL found in email", details)


class InvalidFileError(DownloadError):
    """Raised when downloaded file is invalid or corrupt"""
    def __init__(self, filepath: str, reason: str = None):
        details = {"filepath": filepath}
        if reason:
            details["reason"] = reason
        super().__init__(f"Invalid file: {filepath}", details)


class ExcelProcessingError(PipelineError):
    """Raised when Excel file processing fails"""
    def __init__(self, filepath: str, reason: str = None):
        details = {"filepath": filepath}
        if reason:
            details["reason"] = reason
        super().__init__(f"Failed to process Excel file: {filepath}", details)


class BigQueryError(PipelineError):
    """Base class for BigQuery-related errors"""
    pass


class BigQueryLoadError(BigQueryError):
    """Raised when BigQuery load operation fails"""
    def __init__(self, table_id: str, reason: str = None, row_count: int = None):
        details = {"table_id": table_id}
        if reason:
            details["reason"] = reason
        if row_count is not None:
            details["row_count"] = row_count
        super().__init__(f"Failed to load data to {table_id}", details)


class SchemaError(BigQueryError):
    """Raised when there's a schema mismatch"""
    def __init__(self, field_name: str, expected_type: str, actual_type: str):
        details = {
            "field": field_name,
            "expected": expected_type,
            "actual": actual_type
        }
        super().__init__(
            f"Schema mismatch for field '{field_name}': expected {expected_type}, got {actual_type}",
            details
        )


class DataValidationError(PipelineError):
    """Raised when data validation fails"""
    def __init__(self, field_name: str, issue: str, sample_value: str = None):
        details = {
            "field": field_name,
            "issue": issue
        }
        if sample_value:
            details["sample"] = str(sample_value)[:100]  # Limit sample length
        super().__init__(f"Data validation failed for '{field_name}': {issue}", details)


class StateError(PipelineError):
    """Raised when state management fails"""
    pass


class ConfigurationError(PipelineError):
    """Raised when configuration is invalid or missing"""
    def __init__(self, config_key: str, reason: str = None):
        details = {"config_key": config_key}
        if reason:
            details["reason"] = reason
        super().__init__(f"Configuration error for '{config_key}'", details)


class RetryableError(PipelineError):
    """Base class for errors that should trigger a retry"""
    def __init__(self, message: str, retry_after: int = None, details: dict = None):
        super().__init__(message, details)
        self.retry_after = retry_after  # Seconds to wait before retry


class RateLimitError(RetryableError):
    """Raised when API rate limit is hit"""
    def __init__(self, api_name: str, retry_after: int = 60):
        super().__init__(
            f"Rate limit exceeded for {api_name}",
            retry_after=retry_after,
            details={"api": api_name}
        )


class TemporaryError(RetryableError):
    """Raised for temporary failures that should be retried"""
    pass


# Helper function to determine if an error is retryable
def is_retryable(error: Exception) -> bool:
    """Check if an error should trigger a retry"""
    return isinstance(error, RetryableError)


# Helper function to get retry delay
def get_retry_delay(error: Exception, default: int = 5) -> int:
    """Get the retry delay in seconds for an error"""
    if isinstance(error, RetryableError) and error.retry_after:
        return error.retry_after
    return default