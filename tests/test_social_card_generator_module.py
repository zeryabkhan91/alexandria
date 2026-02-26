from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from PIL import Image, ImageDraw

from src import social_card_generator as sc


def _img(path: Path, size=(640, 960), color=(120, 90, 60, 255)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGBA", size, color=color).save(path, format="PNG")


def test_text_helpers_and_format_parser():
    canvas = Image.new("RGBA", (400, 300), (0, 0, 0, 0))
    draw = ImageDraw.Draw(canvas, "RGBA")
    font = sc._load_font(20)
    lines = sc._text_wrap(draw, "very long sentence split across words", max_width=120, font=font)
    assert lines

    end_y = sc._draw_text_block(
        draw,
        text="another wrapped block of words",
        start=(10, 10),
        width=140,
        font=font,
        fill=(255, 255, 255, 255),
        line_gap=4,
        max_lines=3,
    )
    assert end_y > 10
    assert sc._parse_formats(" instagram,facebook ") == ["instagram", "facebook"]
    assert sc._parse_formats(None) is None


def test_compose_format_generates_expected_sizes():
    standing_front = Image.new("RGBA", (700, 1000), (200, 180, 150, 255))
    standing_angled = Image.new("RGBA", (700, 1000), (180, 150, 120, 255))

    for fmt, spec in sc.SOCIAL_SPECS.items():
        output = sc._compose_format(
            fmt=fmt,
            book_title="Moby Dick",
            author="Herman Melville",
            standing_front=standing_front,
            standing_angled=standing_angled,
        )
        assert output.mode == "RGB"
        assert output.size == tuple(spec["size"])


def test_load_mockup_or_generate(tmp_path: Path, monkeypatch):
    cover = tmp_path / "cover.jpg"
    Image.new("RGB", (300, 450), (90, 80, 70)).save(cover, format="JPEG")

    def _fake_generate_mockup(**kwargs):  # type: ignore[no-untyped-def]
        target = Path(kwargs["output_path"])
        target.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (320, 480), (120, 100, 90)).save(target, format="JPEG")
        return str(target)

    monkeypatch.setattr(sc.mockup_generator, "generate_mockup", _fake_generate_mockup)
    image = sc._load_mockup_or_generate(
        cover_path=cover,
        book_title="Title",
        book_author="Author",
        template_id="standing_front",
        temp_dir=tmp_path / "tmp_social",
    )
    assert image.mode == "RGBA"


def test_generate_social_cards_for_book(tmp_path: Path, monkeypatch):
    cover = tmp_path / "winner.jpg"
    Image.new("RGB", (400, 600), (90, 70, 60)).save(cover, format="JPEG")

    record = SimpleNamespace(title="Test Title", author="Test Author", folder_name="1. Test Title")
    monkeypatch.setattr(sc.mockup_generator, "load_book_records", lambda: {1: record})
    monkeypatch.setattr(sc.mockup_generator, "load_winner_map", lambda _p: {1: 2})
    monkeypatch.setattr(
        sc.mockup_generator,
        "winner_cover_path",
        lambda **_kwargs: cover,
    )
    monkeypatch.setattr(
        sc,
        "_load_mockup_or_generate",
        lambda **_kwargs: Image.new("RGBA", (600, 900), (200, 180, 150, 255)),
    )

    summary = sc.generate_social_cards_for_book(
        book_number=1,
        formats=["instagram", "invalid"],
        output_root=tmp_path / "Output Covers",
        selections_path=tmp_path / "winner_selections.json",
    )
    assert summary["book"] == 1
    assert len(summary["generated"]) == 1
    assert summary["generated"][0].endswith("instagram.jpg")


def test_generate_social_cards_and_main(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(sc.mockup_generator, "load_winner_map", lambda _p: {1: 2, 2: 1})
    monkeypatch.setattr(
        sc,
        "generate_social_cards_for_book",
        lambda **kwargs: {"book": kwargs["book_number"], "generated": ["x"]},
    )

    summary = sc.generate_social_cards(
        output_dir=str(tmp_path / "Output Covers"),
        selections_path=str(tmp_path / "winner_selections.json"),
        book=1,
        all_books=False,
        formats=["instagram"],
    )
    assert summary["books"] == 1
    assert summary["failed"] == 0
    assert summary["formats"] == ["instagram"]

    args = SimpleNamespace(
        book=1,
        all_books=False,
        formats="instagram,facebook",
        output_dir=tmp_path / "Output Covers",
        selections=tmp_path / "winner_selections.json",
    )
    monkeypatch.setattr(sc.argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(sc, "generate_social_cards", lambda **_kwargs: {"ok": True})
    assert sc.main() == 0

