from __future__ import annotations

import pytest

from src import api_validation


def test_parse_int_valid():
    value, result = api_validation.parse_int("42", field="book")
    assert result.valid is True
    assert value == 42


def test_parse_int_invalid():
    value, result = api_validation.parse_int("not-int", field="book")
    assert value is None
    assert result.valid is False
    assert result.error is not None
    assert result.error.code == "INVALID_INTEGER"


def test_validate_book_number_positive():
    result = api_validation.validate_book_number(1)
    assert result.valid is True


def test_validate_book_number_out_of_range():
    result = api_validation.validate_book_number(0)
    assert result.valid is False
    assert result.error is not None
    assert result.error.code == "BOOK_NUMBER_OUT_OF_RANGE"


def test_validate_book_number_not_in_catalog():
    result = api_validation.validate_book_number(8, valid_books={1, 2, 3})
    assert result.valid is False
    assert result.error is not None
    assert result.error.code == "BOOK_NOT_IN_CATALOG"


def test_validate_positive_int():
    assert api_validation.validate_positive_int(5, field="variants").valid is True
    assert api_validation.validate_positive_int(0, field="variants").valid is False


def test_validate_threshold():
    assert api_validation.validate_threshold("0.5").valid is True
    fail = api_validation.validate_threshold("2.5")
    assert fail.valid is False
    assert fail.error is not None
    assert fail.error.code == "FLOAT_OUT_OF_RANGE"


def test_validate_enum():
    assert api_validation.validate_enum("catalog", field="mode", valid_values={"catalog", "contact_sheet"}).valid is True
    fail = api_validation.validate_enum("bad", field="mode", valid_values={"catalog", "contact_sheet"})
    assert fail.valid is False
    assert fail.error is not None
    assert fail.error.code == "INVALID_ENUM"


def test_validate_non_empty_text():
    assert api_validation.validate_non_empty_text("x", field="name").valid is True
    fail = api_validation.validate_non_empty_text("   ", field="name")
    assert fail.valid is False
    assert fail.error is not None
    assert fail.error.code == "EMPTY_VALUE"


def test_validate_non_empty_text_rejects_null_bytes_and_long_values():
    null_fail = api_validation.validate_non_empty_text("bad\x00value", field="name")
    assert null_fail.valid is False
    assert null_fail.error is not None
    assert null_fail.error.code == "INVALID_TEXT"

    long_fail = api_validation.validate_non_empty_text("x" * 1001, field="name", max_length=1000)
    assert long_fail.valid is False
    assert long_fail.error is not None
    assert long_fail.error.code == "VALUE_TOO_LONG"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1", 1),
        (2, 2),
        ("003", 3),
        ("-4", -4),
        (0, 0),
    ],
)
def test_parse_int_matrix_valid(raw, expected):
    value, result = api_validation.parse_int(raw, field="book")
    assert result.valid is True
    assert value == expected


@pytest.mark.parametrize("raw", ["", "abc", None, "2.5", object()])
def test_parse_int_matrix_invalid(raw):
    value, result = api_validation.parse_int(raw, field="book")
    assert value is None
    assert result.valid is False


@pytest.mark.parametrize(
    ("raw", "is_valid"),
    [
        ("0.0", True),
        ("1.0", True),
        ("0.5", True),
        ("-0.1", False),
        ("1.1", False),
        ("bad", False),
    ],
)
def test_validate_threshold_boundary_cases(raw, is_valid):
    result = api_validation.validate_threshold(raw)
    assert result.valid is is_valid


@pytest.mark.parametrize("raw", ["nan", "inf", "-inf"])
def test_validate_threshold_rejects_non_finite(raw):
    result = api_validation.validate_threshold(raw)
    assert result.valid is False
    assert result.error is not None
    assert result.error.code == "INVALID_FLOAT"
