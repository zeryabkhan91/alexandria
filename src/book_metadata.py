"""Per-book tags and notes storage."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src import safe_json


def metadata_path(*, data_dir: Path, catalog_id: str) -> Path:
    if catalog_id == "classics":
        return data_dir / "book_metadata.json"
    return data_dir / f"book_metadata_{catalog_id}.json"


def _load(path: Path) -> dict[str, Any]:
    payload = safe_json.load_json(path, {"books": {}})
    if not isinstance(payload, dict):
        payload = {"books": {}}
    books = payload.get("books")
    if not isinstance(books, dict):
        payload["books"] = {}
    return payload


def _save(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    safe_json.atomic_write_json(path, payload)


def get_book(path: Path, book_number: int) -> dict[str, Any]:
    payload = _load(path)
    books = payload.get("books", {})
    row = books.get(str(int(book_number)), {})
    if not isinstance(row, dict):
        row = {}
    tags = row.get("tags")
    if not isinstance(tags, list):
        tags = []
    notes = row.get("notes")
    if not isinstance(notes, str):
        notes = ""
    return {"tags": sorted({str(item).strip() for item in tags if str(item).strip()}), "notes": notes}


def set_book(path: Path, book_number: int, *, tags: list[str] | None = None, notes: str | None = None) -> dict[str, Any]:
    payload = _load(path)
    books = payload.get("books", {})
    key = str(int(book_number))
    current = books.get(key, {})
    if not isinstance(current, dict):
        current = {}

    if tags is not None:
        current["tags"] = sorted({str(item).strip() for item in tags if str(item).strip()})
    else:
        current.setdefault("tags", [])

    if notes is not None:
        current["notes"] = str(notes)
    else:
        current.setdefault("notes", "")

    books[key] = current
    payload["books"] = books
    _save(path, payload)
    return {"book": int(book_number), **current}


def add_tags(path: Path, book_number: int, tags: list[str]) -> dict[str, Any]:
    current = get_book(path, book_number)
    merged = set(current.get("tags", []))
    merged.update(str(item).strip() for item in tags if str(item).strip())
    return set_book(path, book_number, tags=sorted(merged), notes=current.get("notes", ""))


def remove_tag(path: Path, book_number: int, tag: str) -> dict[str, Any]:
    current = get_book(path, book_number)
    wanted = str(tag).strip().lower()
    remaining = [item for item in current.get("tags", []) if str(item).strip().lower() != wanted]
    return set_book(path, book_number, tags=remaining, notes=current.get("notes", ""))


def list_books(path: Path) -> dict[str, dict[str, Any]]:
    payload = _load(path)
    books = payload.get("books", {})
    if not isinstance(books, dict):
        books = {}
    out: dict[str, dict[str, Any]] = {}
    for key, row in books.items():
        if not isinstance(row, dict):
            continue
        tags = row.get("tags")
        notes = row.get("notes")
        out[str(key)] = {
            "tags": sorted({str(item).strip() for item in tags}) if isinstance(tags, list) else [],
            "notes": str(notes) if isinstance(notes, str) else "",
        }
    return out


def filter_books_by_tags(path: Path, tags: list[str]) -> list[int]:
    wanted = {str(item).strip().lower() for item in tags if str(item).strip()}
    if not wanted:
        return []
    books = list_books(path)
    out: list[int] = []
    for key, row in books.items():
        try:
            number = int(key)
        except ValueError:
            continue
        current = {str(item).strip().lower() for item in row.get("tags", []) if str(item).strip()}
        if wanted.issubset(current):
            out.append(number)
    return sorted(out)

