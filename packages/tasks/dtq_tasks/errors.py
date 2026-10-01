"""Handler-raised errors controlling retry classification (contract section 4)."""


class RetryableError(Exception):
    """Raised by a handler to mark the failure retryable (backoff applies)."""


class PermanentError(Exception):
    """Raised by a handler to mark the failure permanent (no retry)."""
