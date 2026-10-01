"""HTTP error mapping for the DTQ API.

DTQ contract section 8: every error body is {code, message, request_id}.
Status codes come from the shared dtq_core.errors hierarchy's http_status;
the API adds a Retry-After header on QUEUE_FULL responses.
"""

from __future__ import annotations

from dtq_core.errors import DtqError, QueueFullError
from dtq_core.models import InvalidTransition
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


async def dtq_error_handler(request: Request, exc: DtqError) -> JSONResponse:
    headers: dict[str, str] | None = None
    if isinstance(exc, QueueFullError) and exc.retry_after_s is not None:
        headers = {"Retry-After": str(exc.retry_after_s)}
    return JSONResponse(
        status_code=exc.http_status,
        content={
            "code": exc.code,
            "message": str(exc),
            "request_id": request.headers.get("x-request-id", ""),
        },
        headers=headers,
    )


async def invalid_transition_handler(request: Request, exc: InvalidTransition) -> JSONResponse:
    """Defensive: a state-transition race becomes a 409, never a 500."""
    return JSONResponse(
        status_code=409,
        content={
            "code": "INVALID_TRANSITION",
            "message": str(exc),
            "request_id": request.headers.get("x-request-id", ""),
        },
    )


async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Pydantic body validation failures -> 400 INVALID_TASK (contract 8.2)."""
    details = "; ".join(
        f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}"
        for e in exc.errors()
        if e.get("type") != "missing"
    )
    return JSONResponse(
        status_code=400,
        content={
            "code": "INVALID_TASK",
            "message": details or "request body validation failed",
            "request_id": request.headers.get("x-request-id", ""),
        },
    )


async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={
            "code": "INTERNAL_ERROR",
            "message": "unexpected server error",
            "request_id": request.headers.get("x-request-id", ""),
        },
    )


def register_error_handlers(app) -> None:  # type: ignore[no-untyped-def]
    app.add_exception_handler(DtqError, dtq_error_handler)
    app.add_exception_handler(InvalidTransition, invalid_transition_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.add_exception_handler(Exception, unhandled_error_handler)
