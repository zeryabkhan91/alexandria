from __future__ import annotations

from pathlib import Path

import pytest

from src import security


def test_sanitize_string_strips_nulls_and_limits():
    assert security.sanitize_string("  hello\x00world  ", max_length=20) == "helloworld"
    assert security.sanitize_string("abcdef", max_length=3) == "abc"


def test_sanitize_path_blocks_traversal_and_escape(tmp_path: Path):
    root = tmp_path / "root"
    root.mkdir()
    safe = security.sanitize_path("folder/file.txt", root)
    assert str(safe).endswith("folder/file.txt")
    with pytest.raises(ValueError):
        security.sanitize_path("", root)
    with pytest.raises(ValueError):
        security.sanitize_path("../etc/passwd", root)
    with pytest.raises(ValueError):
        security.sanitize_path(str(tmp_path / "outside.txt"), root)


def test_validate_book_number_and_catalog_id():
    assert security.validate_book_number("12") == 12
    with pytest.raises(ValueError):
        security.validate_book_number("abc")
    with pytest.raises(ValueError):
        security.validate_book_number(0, min_value=1, max_value=100)
    assert security.validate_catalog_id("classic-lit") == "classic-lit"
    with pytest.raises(ValueError):
        security.validate_catalog_id("")
    with pytest.raises(ValueError):
        security.validate_catalog_id("bad id")


def test_mask_api_key_and_scrub_sensitive():
    assert security.mask_api_key("sk-1234567890") == "sk-1...7890"
    assert security.mask_api_key("abcd") == "****"
    assert security.mask_api_key("") == ""
    payload = {"api_key": "secret", "nested": {"token": "abc", "value": 1}, "list": [{"password": "x"}]}
    scrubbed = security.scrub_sensitive(payload)
    assert scrubbed["api_key"] == "[REDACTED]"
    assert scrubbed["nested"]["token"] == "[REDACTED]"
    assert scrubbed["list"][0]["password"] == "[REDACTED]"
