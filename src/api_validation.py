"""Input validation helpers for API endpoints."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any


@dataclass(slots=True)
class ValidationError:
    code: str
    message: str
    details: dict[str, Any]


@dataclass(slots=True)
class ValidationResult:
    valid: bool
    error: ValidationError | None = None


def ok() -> ValidationResult:
    return ValidationResult(valid=True, error=None)


def fail(code: str, message: str, details: dict[str, Any] | None = None) -> ValidationResult:
    return ValidationResult(valid=False, error=ValidationError(code=code, message=message, details=details or {}))


def parse_int(value: Any, *, field: str) -> tuple[int | None, ValidationResult]:
    try:
        return int(value), ok()
    except (TypeError, ValueError):
        return None, fail(
            "INVALID_INTEGER",
            f"{field} must be an integer",
            {"field": field, "received": str(value)},
        )


def validate_book_number(value: Any, *, valid_books: set[int] | None = None) -> ValidationResult:
    number, parsed = parse_int(value, field="book")
    if not parsed.valid or number is None:
        return parsed

    if number <= 0:
        return fail(
            "BOOK_NUMBER_OUT_OF_RANGE",
            "book must be a positive integer",
            {"received": number},
        )

    if valid_books is not None and number not in valid_books:
        return fail(
            "BOOK_NOT_IN_CATALOG",
            "book is not in the active catalog",
            {"received": number},
        )

    return ok()


def validate_positive_int(value: Any, *, field: str) -> ValidationResult:
    number, parsed = parse_int(value, field=field)
    if not parsed.valid or number is None:
        return parsed
    if number <= 0:
        return fail(
            "INTEGER_OUT_OF_RANGE",
            f"{field} must be a positive integer",
            {"field": field, "received": number},
        )
    return ok()


def validate_threshold(value: Any, *, field: str = "threshold", low: float = 0.0, high: float = 1.0) -> ValidationResult:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return fail(
            "INVALID_FLOAT",
            f"{field} must be a numeric value",
            {"field": field, "received": str(value)},
        )
    if not math.isfinite(parsed):
        return fail(
            "INVALID_FLOAT",
            f"{field} must be a finite numeric value",
            {"field": field, "received": str(value)},
        )
    if parsed < low or parsed > high:
        return fail(
            "FLOAT_OUT_OF_RANGE",
            f"{field} must be between {low} and {high}",
            {"field": field, "received": parsed, "range": [low, high]},
        )
    return ok()


def validate_enum(value: Any, *, field: str, valid_values: set[str]) -> ValidationResult:
    token = str(value or "").strip()
    if token in valid_values:
        return ok()
    return fail(
        "INVALID_ENUM",
        f"{field} must be one of: {', '.join(sorted(valid_values))}",
        {"field": field, "received": token, "valid_values": sorted(valid_values)},
    )


def validate_non_empty_text(value: Any, *, field: str, max_length: int = 1000) -> ValidationResult:
    token = str(value or "").strip()
    if "\x00" in token:
        return fail(
            "INVALID_TEXT",
            f"{field} contains invalid null bytes",
            {"field": field},
        )
    if max_length > 0 and len(token) > int(max_length):
        return fail(
            "VALUE_TOO_LONG",
            f"{field} exceeds maximum length of {max_length}",
            {"field": field, "max_length": int(max_length), "received_length": len(token)},
        )
    if token:
        return ok()
    return fail(
        "EMPTY_VALUE",
        f"{field} cannot be empty",
        {"field": field},
    )
