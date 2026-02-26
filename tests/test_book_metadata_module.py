from __future__ import annotations

from pathlib import Path

from src import book_metadata


def test_metadata_path_for_classics_and_custom_catalog(tmp_path: Path):
    assert book_metadata.metadata_path(data_dir=tmp_path, catalog_id="classics") == tmp_path / "book_metadata.json"
    assert book_metadata.metadata_path(data_dir=tmp_path, catalog_id="sci-fi") == tmp_path / "book_metadata_sci-fi.json"


def test_get_book_defaults_when_file_missing(tmp_path: Path):
    path = tmp_path / "book_metadata.json"
    payload = book_metadata.get_book(path, 12)
    assert payload == {"tags": [], "notes": ""}


def test_set_book_writes_tags_and_notes(tmp_path: Path):
    path = tmp_path / "book_metadata.json"
    row = book_metadata.set_book(path, 5, tags=["priority", "fiction"], notes="Needs cooler tones.")
    assert row["book"] == 5
    assert row["notes"] == "Needs cooler tones."
    assert sorted(row["tags"]) == ["fiction", "priority"]
    loaded = book_metadata.get_book(path, 5)
    assert loaded["notes"] == "Needs cooler tones."
    assert loaded["tags"] == ["fiction", "priority"]


def test_add_tags_deduplicates_and_preserves_notes(tmp_path: Path):
    path = tmp_path / "book_metadata.json"
    book_metadata.set_book(path, 3, tags=["classic"], notes="Original note")
    updated = book_metadata.add_tags(path, 3, ["classic", "priority-high", "classic"])
    assert updated["notes"] == "Original note"
    assert updated["tags"] == ["classic", "priority-high"]


def test_remove_tag_is_case_insensitive(tmp_path: Path):
    path = tmp_path / "book_metadata.json"
    book_metadata.set_book(path, 7, tags=["Sci-Fi", "Priority"], notes="")
    updated = book_metadata.remove_tag(path, 7, "sci-fi")
    assert updated["tags"] == ["Priority"]


def test_list_books_normalizes_invalid_rows(tmp_path: Path):
    path = tmp_path / "book_metadata.json"
    book_metadata.set_book(path, 1, tags=["A", "A", "B"], notes="One")
    book_metadata.set_book(path, 2, tags=["C"], notes="Two")
    rows = book_metadata.list_books(path)
    assert set(rows.keys()) == {"1", "2"}
    assert rows["1"]["tags"] == ["A", "B"]
    assert rows["2"]["notes"] == "Two"


def test_filter_books_by_tags_requires_all_tags(tmp_path: Path):
    path = tmp_path / "book_metadata.json"
    book_metadata.set_book(path, 11, tags=["fiction", "priority-high"], notes="")
    book_metadata.set_book(path, 12, tags=["fiction"], notes="")
    assert book_metadata.filter_books_by_tags(path, ["fiction", "priority-high"]) == [11]
    assert book_metadata.filter_books_by_tags(path, ["fiction"]) == [11, 12]
    assert book_metadata.filter_books_by_tags(path, []) == []


def test_set_book_preserves_existing_values_when_partial_update(tmp_path: Path):
    path = tmp_path / "book_metadata.json"
    book_metadata.set_book(path, 9, tags=["a"], notes="n1")
    book_metadata.set_book(path, 9, notes="n2")
    row = book_metadata.get_book(path, 9)
    assert row["tags"] == ["a"]
    assert row["notes"] == "n2"


def test_load_handles_non_dict_root_and_non_dict_books(tmp_path: Path):
    path = tmp_path / "book_metadata.json"
    path.write_text("[]", encoding="utf-8")
    assert book_metadata.get_book(path, 1) == {"tags": [], "notes": ""}

    path.write_text('{"books":"bad"}', encoding="utf-8")
    assert book_metadata.get_book(path, 1) == {"tags": [], "notes": ""}
    assert book_metadata.list_books(path) == {}


def test_get_and_set_handle_non_dict_book_rows(tmp_path: Path):
    path = tmp_path / "book_metadata.json"
    path.write_text('{"books":{"5":"not-a-dict"}}', encoding="utf-8")

    assert book_metadata.get_book(path, 5) == {"tags": [], "notes": ""}

    updated = book_metadata.set_book(path, 5)
    assert updated["book"] == 5
    assert updated["tags"] == []
    assert updated["notes"] == ""


def test_filter_books_by_tags_skips_non_numeric_keys(tmp_path: Path):
    path = tmp_path / "book_metadata.json"
    path.write_text(
        '{"books":{"bad-key":{"tags":["fiction"],"notes":""},"3":{"tags":["fiction"],"notes":""}}}',
        encoding="utf-8",
    )
    assert book_metadata.filter_books_by_tags(path, ["fiction"]) == [3]


def test_list_books_handles_non_dict_books(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(book_metadata, "_load", lambda _path: {"books": "bad"})
    assert book_metadata.list_books(tmp_path / "book_metadata.json") == {}


def test_list_books_skips_non_dict_rows(tmp_path: Path):
    path = tmp_path / "book_metadata_rows.json"
    path.write_text(
        '{"books":{"bad":"not-a-dict","2":{"tags":["A","A"],"notes":"ok"}}}',
        encoding="utf-8",
    )
    rows = book_metadata.list_books(path)
    assert rows == {"2": {"tags": ["A"], "notes": "ok"}}
