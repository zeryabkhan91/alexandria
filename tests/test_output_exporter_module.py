from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image

from src import output_exporter as oe


def _make_image(path: Path, size=(800, 600), color=(100, 150, 200)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, color=color).save(path, format="JPEG")


def test_export_jpg_pdf_ai_and_inspect(tmp_path: Path):
    source = tmp_path / "source.jpg"
    _make_image(source)

    jpg = tmp_path / "out.jpg"
    pdf = tmp_path / "out.pdf"
    ai = tmp_path / "out.ai"

    oe.export_jpg(source, jpg)
    oe.export_pdf(source, pdf)
    oe.export_ai(source, ai)

    assert jpg.exists() and pdf.exists() and ai.exists()
    assert pdf.read_bytes().startswith(b"%PDF")
    info = oe.inspect_ai_internal_format(ai)
    assert info["is_pdf_based"] is True


def test_export_variant(tmp_path: Path):
    source = tmp_path / "source.jpg"
    _make_image(source)
    variant_dir = tmp_path / "Variant-1"
    files = oe.export_variant(source, variant_dir, "Book Title")
    assert len(files) == 3
    assert all(path.exists() for path in files)


def test_fallback_collect_variant_images(tmp_path: Path, monkeypatch):
    composited = tmp_path / "composited" / "1"
    _make_image(composited / "model_a" / "variant_1.jpg")
    _make_image(composited / "model_b" / "variant_2.jpg")
    _make_image(composited / "model_c" / "variant_3.jpg")

    monkeypatch.setattr(oe.config, "get_config", lambda: SimpleNamespace(max_export_variants=2))
    selected = oe._fallback_collect_variant_images(composited)
    assert len(selected) == 2
    assert selected[0].name.startswith("variant_")


def test_export_book_variants_and_batch_export(tmp_path: Path):
    composited_root = tmp_path / "tmp" / "composited"
    output_root = tmp_path / "Output Covers"
    catalog_path = tmp_path / "catalog.json"

    catalog = [{"number": 1, "folder_name": "1. Test Book", "file_base": "Test Book"}]
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")

    _make_image(composited_root / "1" / "variant_1.jpg")
    _make_image(composited_root / "1" / "variant_2.jpg")

    exported = oe.export_book_variants(
        book_number=1,
        composited_root=composited_root,
        output_root=output_root,
        catalog_path=catalog_path,
    )
    assert len(exported) == 6
    assert (output_root / "1. Test Book" / "Variant-1" / "Test Book.jpg").exists()

    summary = oe.batch_export(
        composited_root=composited_root,
        output_root=output_root,
        books=[1],
        max_books=1,
    )
    assert summary["processed_books"] == 1
    assert summary["failed_books"] == 0
    assert summary["files_exported"] >= 6


def test_parse_helpers():
    assert oe._parse_variant("variant_4") == 4
    assert oe._parse_variant("bad") == 0
    assert oe._parse_books("1,3-4") == [1, 3, 4]


def test_inspect_ai_internal_format_missing_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        oe.inspect_ai_internal_format(tmp_path / "missing.ai")


def test_export_pdf_fallback_when_reportlab_unavailable(tmp_path: Path, monkeypatch):
    source = tmp_path / "source.jpg"
    _make_image(source)
    out_pdf = tmp_path / "fallback.pdf"

    monkeypatch.setattr(oe, "REPORTLAB_AVAILABLE", False)
    oe.export_pdf(source, out_pdf)
    assert out_pdf.exists()
    assert out_pdf.read_bytes().startswith(b"%PDF")


def test_export_book_variants_missing_catalog_or_composited_dir(tmp_path: Path):
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps([{"number": 7, "folder_name": "7. Book", "file_base": "Book"}]),
        encoding="utf-8",
    )

    with pytest.raises(KeyError):
        oe.export_book_variants(
            book_number=8,
            composited_root=tmp_path / "composited",
            output_root=tmp_path / "out",
            catalog_path=catalog_path,
        )

    with pytest.raises(FileNotFoundError):
        oe.export_book_variants(
            book_number=7,
            composited_root=tmp_path / "composited",
            output_root=tmp_path / "out",
            catalog_path=catalog_path,
        )


def test_export_book_variants_skips_invalid_variant_names(tmp_path: Path):
    composited_root = tmp_path / "composited"
    output_root = tmp_path / "out"
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps([{"number": 1, "folder_name": "1. Book copy", "file_base": "Book"}]),
        encoding="utf-8",
    )
    _make_image(composited_root / "1" / "variant_1.jpg")
    _make_image(composited_root / "1" / "variant_bad.jpg")

    exported = oe.export_book_variants(
        book_number=1,
        composited_root=composited_root,
        output_root=output_root,
        catalog_path=catalog_path,
    )
    assert len(exported) == 3
    assert (output_root / "1. Book" / "Variant-1" / "Book.jpg").exists()


def test_fallback_collect_variant_images_deduplicates_variant_ids(tmp_path: Path, monkeypatch):
    composited = tmp_path / "composited" / "1"
    _make_image(composited / "model_a" / "variant_1.jpg")
    _make_image(composited / "model_b" / "variant_1.jpg")
    _make_image(composited / "model_b" / "variant_2.jpg")
    _make_image(composited / "model_c" / "variant_bad.jpg")

    monkeypatch.setattr(oe.config, "get_config", lambda: SimpleNamespace(max_export_variants=10))
    selected = oe._fallback_collect_variant_images(composited)
    assert [path.stem for path in selected] == ["variant_1", "variant_2"]


def test_batch_export_default_scope_and_error_capture(tmp_path: Path, monkeypatch):
    composited_root = tmp_path / "composited"
    output_root = tmp_path / "out"
    (composited_root / "2").mkdir(parents=True, exist_ok=True)
    (composited_root / "5").mkdir(parents=True, exist_ok=True)

    def _fake_export_book_variants(*, book_number, **_kwargs):
        if book_number == 2:
            raise RuntimeError("boom")
        return [Path("a"), Path("b")]

    monkeypatch.setattr(oe, "export_book_variants", _fake_export_book_variants)

    summary = oe.batch_export(
        composited_root=composited_root,
        output_root=output_root,
        books=None,
        max_books=2,
    )
    assert summary["processed_books"] == 2
    assert summary["failed_books"] == 1
    assert summary["success_books"] == 1
    assert summary["files_exported"] == 2
    assert summary["errors"][0]["book_number"] == 2


def test_parse_books_none_and_reverse_ranges():
    assert oe._parse_books(None) is None
    assert oe._parse_books("5-3,2") == [2, 3, 4, 5]


def test_main_inspect_book_and_batch_paths(monkeypatch, tmp_path: Path):
    inspect_args = SimpleNamespace(
        composited_root=tmp_path / "composited",
        output_root=tmp_path / "out",
        book=None,
        books=None,
        max_books=20,
        max_variants=None,
        inspect_ai=tmp_path / "probe.ai",
    )
    inspect_args.inspect_ai.write_bytes(b"%PDF-1.7\n")
    monkeypatch.setattr(oe.argparse.ArgumentParser, "parse_args", lambda self: inspect_args)
    assert oe.main() == 0

    book_args = SimpleNamespace(**inspect_args.__dict__)
    book_args.inspect_ai = None
    book_args.book = 7
    monkeypatch.setattr(oe.argparse.ArgumentParser, "parse_args", lambda self: book_args)
    monkeypatch.setattr(oe, "export_book_variants", lambda **_kwargs: [Path("x"), Path("y"), Path("z")])
    assert oe.main() == 0

    batch_args = SimpleNamespace(**inspect_args.__dict__)
    batch_args.inspect_ai = None
    batch_args.book = None
    batch_args.books = "1,2-3"
    monkeypatch.setattr(oe.argparse.ArgumentParser, "parse_args", lambda self: batch_args)
    monkeypatch.setattr(oe, "batch_export", lambda **_kwargs: {"processed_books": 3})
    assert oe.main() == 0
