from __future__ import annotations

import argparse
import io
import json
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont
import pytest

from src import mockup_generator as mg


def _img(path: Path, size=(800, 600), color=(80, 90, 110), mode="RGB"):
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new(mode, size, color=color).save(path, format="PNG" if path.suffix.lower() == ".png" else "JPEG")


def _template(template_id: str = "standing_front") -> mg.MockupTemplate:
    return mg.MockupTemplate(
        id=template_id,
        name="Template",
        category="product",
        description="desc",
        base_image=Path("/tmp/base.png"),
        mask_image=Path("/tmp/mask.png"),
        transform={"cover_corners": [[100, 80], [320, 80], [320, 420], [100, 420]]},
        output_size=(640, 480),
        use_case="test",
    )


def test_basic_helpers_and_parsers():
    assert mg._normalise_folder_name("Book copy") == "Book"
    assert mg._normalise_folder_name("Book") == "Book"
    assert mg._parse_size("1600x1200", (1000, 1000)) == (1600, 1200)
    assert mg._parse_size("bad", (900, 700)) == (900, 700)
    assert mg._safe_int("5", 0) == 5
    assert mg._safe_int("bad", 7) == 7
    assert mg._parse_csv_ints("1,2,a") == [1, 2]
    assert mg._parse_csv_tokens("a,b,,c") == ["a", "b", "c"]


def test_load_templates_and_prompts(tmp_path: Path):
    cfg = tmp_path / "templates.json"
    payload = {
        "templates": [
            {
                "id": "t1",
                "name": "T1",
                "category": "product",
                "description": "x",
                "base_image": "config/mockup_templates/t1_base.png",
                "mask_image": "config/mockup_templates/t1_mask.png",
                "transform": {"cover_corners": [[1, 1], [10, 1], [10, 10], [1, 10]]},
                "output_size": [800, 600],
                "use_case": "catalog",
            }
        ]
    }
    cfg.write_text(json.dumps(payload), encoding="utf-8")
    templates = mg.load_templates(cfg)
    assert len(templates) == 1
    assert templates[0].id == "t1"
    tmap = mg.template_map(cfg)
    assert "t1" in tmap

    prompts_path = tmp_path / "prompts.json"
    prompts_path.write_text(json.dumps({"desk_scene": {"prompt": "x", "negative": "y", "size": "1600x1200"}}), encoding="utf-8")
    prompts = mg.load_background_prompts(prompts_path)
    assert "desk_scene" in prompts


def test_build_base_scene_and_mask_image():
    t = _template()
    scene = mg._build_base_scene(t)
    assert scene.size == t.output_size

    t2 = mg.MockupTemplate(
        id="stack_three",
        name="Stack",
        category="product",
        description="x",
        base_image=Path("/tmp/base2.png"),
        mask_image=Path("/tmp/mask2.png"),
        transform={"stack": [{"cover_corners": [[10, 10], [100, 10], [100, 120], [10, 120]]}]},
        output_size=(300, 300),
        use_case="x",
    )
    mask = mg._build_mask_image(t2)
    assert mask.size == (300, 300)


def test_points_warp_shadow_lighting_and_highlight():
    pts = mg._points([[0, 0], [100, 0], [100, 100], [0, 100]])
    assert len(pts) == 4
    assert mg._points("bad") == []

    source = Image.new("RGBA", (100, 120), (200, 100, 50, 255))
    warped = mg._warp_image(source, pts, (200, 200))
    assert warped.size == (200, 200)

    shadow = mg._book_shadow(warped, offset_x=5, offset_y=5, blur=8, opacity=0.4)
    assert shadow.size == warped.size

    lit = mg._apply_lighting(warped, direction="top-left", intensity=0.2)
    assert lit.size == warped.size

    before = warped.copy()
    mg._add_page_edge_highlight(warped, pts)
    assert warped.tobytes() != before.tobytes()


def test_extract_cover_regions_and_amazon_renderers():
    cover = Image.new("RGB", (1200, 800), (120, 100, 90))
    front, spine, back, detail = mg._extract_cover_regions(cover, spine_width_px=120)
    assert front.size[0] == 600
    assert back.size[0] == 600
    assert spine.size[0] > 0
    assert detail.size[0] > 0 and detail.size[1] > 0

    main = mg._render_amazon_main(front)
    back_img = mg._render_amazon_back(back)
    spine_img = mg._render_amazon_spine(spine)
    detail_img = mg._render_amazon_detail(detail)
    assert main.size == (2560, 2560)
    assert back_img.size == (2560, 2560)
    assert spine_img.size == (2560, 2560)
    assert detail_img.size == (2560, 2560)


def test_load_winner_map_and_resolve_base_image(tmp_path: Path, monkeypatch):
    winners_path = tmp_path / "winners.json"
    winners_path.write_text(json.dumps({"selections": {"1": {"winner": 2}, "2": 1}}), encoding="utf-8")
    winners = mg.load_winner_map(winners_path)
    assert winners == {1: 2, 2: 1}

    custom_dir = tmp_path / "custom"
    custom_dir.mkdir(parents=True, exist_ok=True)
    custom = custom_dir / "standing_front_custom.jpg"
    _img(custom)
    monkeypatch.setattr(mg, "CUSTOM_BACKGROUNDS_DIR", custom_dir)
    t = _template("standing_front")
    assert mg._resolve_base_image(t) == custom


def test_zip_and_status(tmp_path: Path, monkeypatch):
    output_dir = tmp_path / "Output Covers"
    record = mg.BookRecord(number=1, title="Book", author="Author", folder_name="1. Book")
    monkeypatch.setattr(mg, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(mg, "load_book_records", lambda *args, **kwargs: {1: record})
    monkeypatch.setattr(mg, "load_templates", lambda *args, **kwargs: [_template("standing_front"), _template("desk_scene")])

    mockup_dir = output_dir / "Mockups" / "1. Book"
    _img(mockup_dir / "standing_front.jpg")
    _img(mockup_dir / "desk_scene.jpg")

    zip_path = mg.build_mockup_zip(book_number=1, output_dir=output_dir, destination=tmp_path / "book1.zip")
    assert zip_path.exists()

    status = mg.mockup_status(output_dir=output_dir)
    assert status["total_books"] == 1
    assert status["complete_books"] == 1


def test_generate_all_mockups_and_amazon_sets_with_stubs(tmp_path: Path, monkeypatch):
    output_root = tmp_path / "Output Covers"
    selections = tmp_path / "winners.json"
    selections.write_text(json.dumps({"selections": {"1": {"winner": 1}}}), encoding="utf-8")
    record = mg.BookRecord(number=1, title="Book", author="Author", folder_name="1. Book")
    cover_path = output_root / "1. Book" / "Variant-1" / "cover.jpg"
    _img(cover_path, size=(1200, 800))

    monkeypatch.setattr(mg, "template_map", lambda *args, **kwargs: {"standing_front": _template("standing_front")})
    monkeypatch.setattr(mg, "ensure_template_assets", lambda **kwargs: {"ok": True})
    monkeypatch.setattr(mg, "_book_targets", lambda **kwargs: [(record, cover_path)])
    monkeypatch.setattr(mg, "generate_mockup", lambda **kwargs: str(Path(kwargs["output_path"])))
    summary = mg.generate_all_mockups(output_dir=str(output_root), selections_path=str(selections), templates=["standing_front"], books=[1])
    assert summary["books"] == 1
    assert summary["generated"] == 1

    monkeypatch.setattr(
        mg,
        "generate_amazon_set_for_book",
        lambda **kwargs: {"book": kwargs["book_number"], "files": ["01_main.jpg"]},
    )
    amazon = mg.generate_amazon_sets(output_dir=str(output_root), selections_path=str(selections), books=[1])
    assert amazon["books"] == 1


def test_ensure_template_assets_and_background_wrapper(tmp_path: Path, monkeypatch):
    base = tmp_path / "assets" / "base.png"
    mask = tmp_path / "assets" / "mask.png"
    t = mg.MockupTemplate(
        id="desk_scene",
        name="Desk",
        category="lifestyle",
        description="x",
        base_image=base,
        mask_image=mask,
        transform={"cover_corners": [[10, 10], [60, 10], [60, 80], [10, 80]]},
        output_size=(640, 480),
        use_case="x",
    )
    monkeypatch.setattr(mg, "load_background_prompts", lambda *args, **kwargs: {"desk_scene": {"prompt": "x", "negative": "y"}})
    monkeypatch.setattr(mg.config, "get_config", lambda: type("R", (), {"ai_provider": "synthetic", "ai_model": "m"})())
    monkeypatch.setattr(mg, "_generate_background_scene", lambda *args, **kwargs: Image.new("RGBA", (640, 480), (10, 20, 30, 255)))

    summary = mg.ensure_template_assets(templates=[t], force=True, generate_backgrounds=True)
    assert summary["bases_created"] == 1
    assert summary["masks_created"] == 1
    assert base.exists() and mask.exists()

    monkeypatch.setattr(mg, "load_templates", lambda: [t])
    monkeypatch.setattr(mg, "ensure_template_assets", lambda **kwargs: {"templates": 1})
    out = mg.generate_backgrounds(force=True)
    assert out["generated_backgrounds"] == 1


def test_render_template_composite_branches(tmp_path: Path):
    front = Image.new("RGB", (500, 700), (130, 110, 90))
    spine = Image.new("RGB", (90, 700), (80, 80, 80))
    back = Image.new("RGB", (500, 700), (100, 90, 80))
    detail = Image.new("RGB", (220, 220), (150, 130, 110))

    # open_spread branch
    open_base = tmp_path / "open_base.png"
    _img(open_base, size=(1200, 800), mode="RGBA", color=(200, 200, 200, 255))
    t_open = mg.MockupTemplate(
        id="open_spread",
        name="Open",
        category="product",
        description="x",
        base_image=open_base,
        mask_image=tmp_path / "open_mask.png",
        transform={
            "left_cover_corners": [[120, 180], [500, 140], [500, 660], [120, 700]],
            "right_cover_corners": [[700, 140], [1080, 180], [1080, 700], [700, 660]],
            "spine_corners": [[600, 150], [660, 150], [660, 690], [600, 690]],
        },
        output_size=(1200, 800),
        use_case="x",
    )
    img_open = mg._render_template_composite(template=t_open, front=front, spine=spine, back=back, detail=detail, title="Title", author="Author")
    assert img_open.size[0] >= 1200

    # stack_three branch
    stack_base = tmp_path / "stack_base.png"
    _img(stack_base, size=(1200, 800), mode="RGBA", color=(180, 180, 180, 255))
    t_stack = mg.MockupTemplate(
        id="stack_three",
        name="Stack",
        category="product",
        description="x",
        base_image=stack_base,
        mask_image=tmp_path / "stack_mask.png",
        transform={
            "stack": [
                {"cover_corners": [[120, 200], [440, 170], [440, 620], [120, 650]], "spine_corners": [[90, 200], [120, 200], [120, 650], [90, 650]], "alpha": 1.0},
                {"cover_corners": [[360, 180], [680, 150], [680, 600], [360, 630]], "spine_corners": [[330, 180], [360, 180], [360, 630], [330, 630]], "alpha": 0.8},
            ]
        },
        output_size=(1200, 800),
        use_case="x",
    )
    img_stack = mg._render_template_composite(template=t_stack, front=front, spine=spine, back=back, detail=detail, title="Title", author="Author")
    assert img_stack.size[0] >= 1200

    # kindle/social branches
    kind_base = tmp_path / "kind_base.png"
    social_base = tmp_path / "social_base.png"
    _img(kind_base, size=(1200, 800), mode="RGBA", color=(190, 190, 190, 255))
    _img(social_base, size=(1200, 675), mode="RGBA", color=(30, 50, 80, 255))
    t_kind = mg.MockupTemplate(
        id="kindle_tablet",
        name="Kindle",
        category="product",
        description="x",
        base_image=kind_base,
        mask_image=tmp_path / "kind_mask.png",
        transform={"cover_corners": [[380, 120], [820, 120], [820, 680], [380, 680]]},
        output_size=(1200, 800),
        use_case="x",
    )
    t_social = mg.MockupTemplate(
        id="social_card",
        name="Social",
        category="social",
        description="x",
        base_image=social_base,
        mask_image=tmp_path / "social_mask.png",
        transform={"cover_corners": [[120, 100], [440, 100], [440, 575], [120, 575]]},
        output_size=(1200, 675),
        use_case="x",
    )
    assert mg._render_template_composite(template=t_kind, front=front, spine=spine, back=back, detail=detail, title="Title", author="Author").size[0] >= 1200
    assert mg._render_template_composite(template=t_social, front=front, spine=spine, back=back, detail=detail, title="Title", author="Author").size[0] >= 1200


def test_generate_mockup_and_book_targets_and_amazon_set(tmp_path: Path, monkeypatch):
    base = tmp_path / "base.png"
    _img(base, size=(1200, 800), mode="RGBA", color=(180, 180, 180, 255))
    template = mg.MockupTemplate(
        id="standing_front",
        name="Standing",
        category="product",
        description="x",
        base_image=base,
        mask_image=tmp_path / "mask.png",
        transform={"cover_corners": [[300, 120], [820, 120], [820, 760], [300, 760]]},
        output_size=(1200, 800),
        use_case="x",
    )
    cover = tmp_path / "cover.jpg"
    _img(cover, size=(1200, 800))

    monkeypatch.setattr(mg, "template_map", lambda *args, **kwargs: {"standing_front": template})
    monkeypatch.setattr(mg, "ensure_template_assets", lambda **kwargs: {"ok": True})
    out = tmp_path / "mockup.jpg"
    saved = mg.generate_mockup(str(cover), "standing_front", str(out), spine_width_px=100, book_title="Book", book_author="Author")
    assert Path(saved).exists()

    catalog = {1: mg.BookRecord(number=1, title="Book", author="Author", folder_name="1. Book")}
    winners = {1: 1}
    output_root = tmp_path / "Output Covers"
    winner_path = output_root / "1. Book" / "Variant-1" / "cover.jpg"
    _img(winner_path, size=(1200, 800))

    monkeypatch.setattr(mg, "load_book_records", lambda *args, **kwargs: catalog)
    monkeypatch.setattr(mg, "load_winner_map", lambda *_a, **_k: winners)
    targets = mg._book_targets(output_root=output_root, selections_path=tmp_path / "winners.json", books=[1])
    assert len(targets) == 1

    # Amazon set branch with mocked generate_mockup outputs.
    def _fake_generate_mockup(**kwargs):
        out_path = Path(kwargs["output_path"])
        _img(out_path, size=(1200, 800))
        return str(out_path)

    monkeypatch.setattr(mg, "generate_mockup", _fake_generate_mockup)
    result = mg.generate_amazon_set_for_book(
        book_number=1,
        output_root=output_root,
        selections_path=tmp_path / "winners.json",
        spine_width_px=100,
    )
    assert result["book"] == 1
    assert len(result["files"]) == 7


def test_load_book_records_and_winner_cover_errors(tmp_path: Path, monkeypatch):
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps({"bad": True}), encoding="utf-8")
    records = mg.load_book_records(catalog_path)
    assert records == {}

    catalog_path.write_text(
        json.dumps(
            [
                {"number": "x"},
                {"number": 2, "title": "Two", "author": "Author", "folder_name": "2. Two copy"},
            ]
        ),
        encoding="utf-8",
    )
    records2 = mg.load_book_records(catalog_path)
    assert 2 in records2
    assert records2[2].folder_name.endswith("2. Two")

    output_root = tmp_path / "Output Covers"
    with pytest.raises(mg.MockupGenerationError):
        mg.winner_cover_path(book_number=9, output_root=output_root, catalog={}, winner_map={})
    with pytest.raises(mg.MockupGenerationError):
        mg.winner_cover_path(book_number=2, output_root=output_root, catalog=records2, winner_map={})
    with pytest.raises(mg.MockupGenerationError):
        mg.winner_cover_path(book_number=2, output_root=output_root, catalog=records2, winner_map={2: 1})


def test_load_winner_map_and_points_edge_cases(tmp_path: Path):
    path = tmp_path / "winners_plain.json"
    path.write_text(json.dumps({"1": 2, "bad": 9, "3": {"winner": "x"}}), encoding="utf-8")
    assert mg.load_winner_map(path) == {1: 2}
    assert mg._points([[1, 2], ["bad"]]) == []


def test_base_scene_and_lifestyle_variants_and_unknown_branch():
    # kindle and social id-specific branches
    kindle = mg.MockupTemplate(
        id="kindle_tablet",
        name="Kindle",
        category="device",
        description="x",
        base_image=Path("/tmp/k.png"),
        mask_image=Path("/tmp/km.png"),
        transform={},
        output_size=(640, 480),
        use_case="x",
    )
    social = mg.MockupTemplate(
        id="social_card",
        name="Social",
        category="social",
        description="x",
        base_image=Path("/tmp/s.png"),
        mask_image=Path("/tmp/sm.png"),
        transform={},
        output_size=(640, 480),
        use_case="x",
    )
    generic = mg.MockupTemplate(
        id="generic",
        name="Generic",
        category="other",
        description="x",
        base_image=Path("/tmp/g.png"),
        mask_image=Path("/tmp/gm.png"),
        transform={},
        output_size=(640, 480),
        use_case="x",
    )
    assert mg._build_base_scene(kindle).size == (640, 480)
    assert mg._build_base_scene(social).size == (640, 480)
    assert mg._build_base_scene(generic).size == (640, 480)

    canvas = Image.new("RGBA", (640, 480))
    draw = ImageDraw.Draw(canvas, "RGBA")
    for template_id in ["desk_scene", "bookshelf", "reading_chair", "window_light", "library_table"]:
        mg._draw_lifestyle_fallback(draw, width=640, height=480, template_id=template_id)


def test_generate_background_scene_openai_size_and_fallback(tmp_path: Path, monkeypatch):
    template = mg.MockupTemplate(
        id="desk_scene",
        name="Desk",
        category="lifestyle",
        description="x",
        base_image=tmp_path / "base.png",
        mask_image=tmp_path / "mask.png",
        transform={},
        output_size=(640, 480),
        use_case="x",
    )

    calls: list[dict] = []

    def _fake_generate_image(**kwargs):  # type: ignore[no-untyped-def]
        calls.append(kwargs)
        img = Image.new("RGBA", (1536, 1024), (10, 20, 30, 255))
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()

    monkeypatch.setattr(mg.image_generator, "generate_image", _fake_generate_image)
    runtime = type("R", (), {"ai_provider": "openai", "ai_model": "x"})()
    out = mg._generate_background_scene(
        template,
        prompt_cfg={"size": "1800x1200", "prompt": "p", "negative": "n"},
        runtime=runtime,
    )
    assert out.size == template.output_size
    assert calls[-1]["params"]["width"] == 1536
    assert calls[-1]["params"]["height"] == 1024

    monkeypatch.setattr(
        mg.image_generator,
        "generate_image",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("x")),
    )
    fallback = mg._generate_background_scene(template, prompt_cfg={"size": "1200x1200"}, runtime=runtime)
    assert fallback.size == template.output_size


def test_warp_and_coeffs_and_lighting_misc_branches():
    src = Image.new("RGBA", (60, 90), (120, 90, 70, 255))
    blank = mg._warp_image(src, [(0, 0)], (120, 120))
    assert blank.size == (120, 120)
    coeffs = mg._find_perspective_coeffs(
        [(0, 0), (59, 0), (59, 89), (0, 89)],
        [(0, 0), (59, 0), (59, 89), (0, 89)],
    )
    assert len(coeffs) == 8

    lit_same = mg._apply_lighting(src, direction="top-left", intensity=0.0)
    assert lit_same.tobytes() == src.tobytes()
    lit_right = mg._apply_lighting(src, direction="right", intensity=0.3)
    lit_center = mg._apply_lighting(src, direction="center", intensity=0.3)
    assert lit_right.size == src.size
    assert lit_center.size == src.size

    before = src.copy()
    mg._add_page_edge_highlight(src, [(1, 1)])
    assert src.tobytes() == before.tobytes()


def test_draw_text_wrap_and_render_rescale_branch(tmp_path: Path):
    img = Image.new("RGBA", (240, 120), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img, "RGBA")
    mg._draw_text_with_wrap(
        draw,
        text="Very long title words that must wrap and clip in tiny box",
        box=(5, 5, 90, 30),
        font=ImageFont.load_default(),
        fill=(255, 255, 255, 255),
    )

    base = tmp_path / "small_base.png"
    _img(base, size=(600, 400), mode="RGBA", color=(180, 180, 180, 255))
    template = mg.MockupTemplate(
        id="default_small",
        name="Default",
        category="product",
        description="x",
        base_image=base,
        mask_image=tmp_path / "mask.png",
        transform={"cover_corners": [[120, 80], [300, 80], [300, 320], [120, 320]], "spine_corners": [[90, 80], [120, 80], [120, 320], [90, 320]]},
        output_size=(600, 400),
        use_case="x",
    )
    front = Image.new("RGB", (300, 400), (120, 100, 90))
    spine = Image.new("RGB", (40, 400), (90, 90, 90))
    back = Image.new("RGB", (300, 400), (100, 90, 80))
    detail = Image.new("RGB", (100, 100), (130, 120, 110))
    result = mg._render_template_composite(
        template=template,
        front=front,
        spine=spine,
        back=back,
        detail=detail,
        title="Title",
        author="Author",
    )
    assert max(result.size) >= 1200


def test_generate_mockup_unknown_template_error(tmp_path: Path, monkeypatch):
    cover = tmp_path / "cover.jpg"
    _img(cover, size=(1000, 700))
    monkeypatch.setattr(mg, "template_map", lambda *args, **kwargs: {"x": _template("x")})
    with pytest.raises(mg.MockupGenerationError):
        mg.generate_mockup(str(cover), "missing", str(tmp_path / "out.jpg"))


def test_book_targets_and_generate_all_mockups_error_paths(tmp_path: Path, monkeypatch):
    output_root = tmp_path / "Output Covers"
    selections = tmp_path / "winners.json"
    selections.write_text(json.dumps({"selections": {"1": {"winner": 1}, "2": {"winner": 1}}}), encoding="utf-8")
    catalog = {
        1: mg.BookRecord(number=1, title="One", author="A", folder_name="1. One"),
    }
    winner_path = output_root / "1. One" / "Variant-1" / "cover.jpg"
    _img(winner_path, size=(1200, 800))

    monkeypatch.setattr(mg, "load_book_records", lambda *args, **kwargs: catalog)
    monkeypatch.setattr(mg, "load_winner_map", lambda *_a, **_k: {1: 1, 2: 1})
    targets = mg._book_targets(output_root=output_root, selections_path=selections, books=[2, 1, 99])
    assert len(targets) == 1

    monkeypatch.setattr(mg, "template_map", lambda *args, **kwargs: {})
    with pytest.raises(mg.MockupGenerationError):
        mg.generate_all_mockups(output_dir=str(output_root), selections_path=str(selections), templates=["nope"], books=[1])

    monkeypatch.setattr(mg, "template_map", lambda *args, **kwargs: {"standing_front": _template("standing_front")})
    monkeypatch.setattr(mg, "ensure_template_assets", lambda **kwargs: {"ok": True})
    monkeypatch.setattr(mg, "_book_targets", lambda **kwargs: [(catalog[1], winner_path)])
    monkeypatch.setattr(mg, "generate_mockup", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    summary = mg.generate_all_mockups(output_dir=str(output_root), selections_path=str(selections), templates=["standing_front"], books=[1])
    assert summary["failed"] == 1


def test_amazon_and_zip_error_paths_and_main_cli(monkeypatch, tmp_path: Path, capsys):
    output_root = tmp_path / "Output Covers"
    selections = tmp_path / "winners.json"
    selections.write_text(json.dumps({"selections": {"1": {"winner": 1}}}), encoding="utf-8")

    monkeypatch.setattr(mg, "load_book_records", lambda *args, **kwargs: {})
    with pytest.raises(mg.MockupGenerationError):
        mg.generate_amazon_set_for_book(book_number=1, output_root=output_root, selections_path=selections)

    monkeypatch.setattr(
        mg,
        "generate_amazon_set_for_book",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("x")),
    )
    summary = mg.generate_amazon_sets(output_dir=str(output_root), selections_path=str(selections), books=[1])
    assert summary["failed"] == 1

    monkeypatch.setattr(mg, "load_book_records", lambda *args, **kwargs: {})
    with pytest.raises(mg.MockupGenerationError):
        mg.build_mockup_zip(book_number=1, output_dir=output_root)

    monkeypatch.setattr(
        mg,
        "load_book_records",
        lambda *args, **kwargs: {1: mg.BookRecord(number=1, title="T", author="A", folder_name="1. T")},
    )
    with pytest.raises(mg.MockupGenerationError):
        mg.build_mockup_zip(book_number=1, output_dir=output_root)

    # main(): generate_backgrounds branch
    args_bg = argparse.Namespace(
        book=None,
        books=None,
        all_books=False,
        template=None,
        output_dir=output_root,
        selections=selections,
        spine_width=100,
        amazon_set=False,
        generate_backgrounds=True,
        force=True,
    )
    monkeypatch.setattr(mg.argparse.ArgumentParser, "parse_args", lambda self: args_bg)
    monkeypatch.setattr(mg, "generate_backgrounds", lambda force=False: {"ok": True, "force": force})
    assert mg.main() == 0
    assert '"ok": true' in capsys.readouterr().out.lower()

    # main(): amazon_set branch
    args_amz = argparse.Namespace(
        book=None,
        books="1,2",
        all_books=False,
        template=None,
        output_dir=output_root,
        selections=selections,
        spine_width=110,
        amazon_set=True,
        generate_backgrounds=False,
        force=False,
    )
    monkeypatch.setattr(mg.argparse.ArgumentParser, "parse_args", lambda self: args_amz)
    monkeypatch.setattr(mg, "generate_amazon_sets", lambda **kwargs: {"ok": True, "books": 2, "kwargs": kwargs})
    assert mg.main() == 0

    # main(): default generate_all_mockups branch
    args_default = argparse.Namespace(
        book=3,
        books=None,
        all_books=False,
        template="standing_front,desk_scene",
        output_dir=output_root,
        selections=selections,
        spine_width=120,
        amazon_set=False,
        generate_backgrounds=False,
        force=False,
    )
    monkeypatch.setattr(mg.argparse.ArgumentParser, "parse_args", lambda self: args_default)
    monkeypatch.setattr(mg, "generate_all_mockups", lambda **kwargs: {"ok": True, "kwargs": kwargs})
    assert mg.main() == 0
