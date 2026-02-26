from __future__ import annotations

import json
from pathlib import Path

import pytest

from src import prompt_library as pl


def _templates_payload() -> dict:
    return {
        "negative_prompt": "text, watermark",
        "style_groups": {
            "sketch_style": {"style_anchors": "sketch anchor"},
            "oil_painting_style": {"style_anchors": "oil anchor"},
            "alternative_style": {"style_anchors": "alt anchor"},
        },
    }


def _library_prompt(prompt_id: str = "p1") -> pl.LibraryPrompt:
    return pl.LibraryPrompt(
        id=prompt_id,
        name="Prompt 1",
        prompt_template="Scene for {title}",
        style_anchors=["warm_sepia_sketch"],
        negative_prompt="text",
        source_book="book",
        source_model="model",
        quality_score=0.8,
        saved_by="tester",
        created_at="2026-02-21T00:00:00+00:00",
        notes="good",
        tags=["iconic", "sketch"],
    )


def test_seed_and_basic_accessors(tmp_path: Path, monkeypatch):
    templates_path = tmp_path / "prompt_templates.json"
    templates_path.write_text(json.dumps(_templates_payload()), encoding="utf-8")
    monkeypatch.setattr(pl.config, "PROMPT_TEMPLATES_PATH", templates_path)

    lib_path = tmp_path / "prompt_library.json"
    library = pl.PromptLibrary(lib_path)
    anchors = library.get_style_anchors()
    prompts = library.get_prompts()
    assert len(anchors) >= 3
    assert len(prompts) >= 1
    assert lib_path.exists()


def test_save_prompt_and_filters(tmp_path: Path, monkeypatch):
    templates_path = tmp_path / "prompt_templates.json"
    templates_path.write_text(json.dumps(_templates_payload()), encoding="utf-8")
    monkeypatch.setattr(pl.config, "PROMPT_TEMPLATES_PATH", templates_path)

    library = pl.PromptLibrary(tmp_path / "prompt_library.json")
    prompt = _library_prompt("custom-1")
    library.save_prompt(prompt)

    all_prompts = library.get_prompts()
    assert any(p.id == "custom-1" for p in all_prompts)

    filtered = library.get_prompts(tags=["iconic"])
    assert any(p.id == "custom-1" for p in filtered)

    searched = library.search_prompts(query="scene", tags=["sketch"], min_quality=0.7)
    assert any(p.id == "custom-1" for p in searched)


def test_save_prompt_requires_title_placeholder(tmp_path: Path, monkeypatch):
    templates_path = tmp_path / "prompt_templates.json"
    templates_path.write_text(json.dumps(_templates_payload()), encoding="utf-8")
    monkeypatch.setattr(pl.config, "PROMPT_TEMPLATES_PATH", templates_path)

    library = pl.PromptLibrary(tmp_path / "prompt_library.json")
    invalid = pl.LibraryPrompt(
        id="bad",
        name="Bad",
        prompt_template="No placeholder here",
        style_anchors=["warm_sepia_sketch"],
        negative_prompt="text",
        source_book="b",
        source_model="m",
        quality_score=0.5,
        saved_by="t",
        created_at="2026-02-21T00:00:00+00:00",
        notes="",
        tags=[],
    )
    with pytest.raises(ValueError):
        library.save_prompt(invalid)


def test_build_prompt_best_prompts_add_anchor(tmp_path: Path, monkeypatch):
    templates_path = tmp_path / "prompt_templates.json"
    templates_path.write_text(json.dumps(_templates_payload()), encoding="utf-8")
    monkeypatch.setattr(pl.config, "PROMPT_TEMPLATES_PATH", templates_path)

    library = pl.PromptLibrary(tmp_path / "prompt_library.json")
    built = library.build_prompt("Moby Dick", ["warm_sepia_sketch"], custom_text="stormy sea")
    assert "Moby Dick" in built
    assert "stormy sea" in built

    with pytest.raises(ValueError):
        library.build_prompt("Moby Dick", ["missing-anchor"])

    library.add_style_anchor(
        pl.StyleAnchor(
            name="new_anchor",
            description="x",
            style_text="new style",
            tags=["new"],
        )
    )
    assert any(a.name == "new_anchor" for a in library.get_style_anchors())

    top = library.get_best_prompts_for_bulk(top_n=3)
    assert len(top) >= 1


def test_reload_existing_library(tmp_path: Path, monkeypatch):
    templates_path = tmp_path / "prompt_templates.json"
    templates_path.write_text(json.dumps(_templates_payload()), encoding="utf-8")
    monkeypatch.setattr(pl.config, "PROMPT_TEMPLATES_PATH", templates_path)

    lib_path = tmp_path / "prompt_library.json"
    first = pl.PromptLibrary(lib_path)
    first.save_prompt(_library_prompt("reload-1"))

    second = pl.PromptLibrary(lib_path)
    assert any(p.id == "reload-1" for p in second.get_prompts())


def test_load_or_seed_recovers_from_invalid_library_json(tmp_path: Path, monkeypatch):
    templates_path = tmp_path / "prompt_templates.json"
    templates_path.write_text(json.dumps(_templates_payload()), encoding="utf-8")
    monkeypatch.setattr(pl.config, "PROMPT_TEMPLATES_PATH", templates_path)

    lib_path = tmp_path / "prompt_library.json"
    lib_path.write_text("{bad", encoding="utf-8")
    library = pl.PromptLibrary(lib_path)

    assert library.get_style_anchors()
    assert library.get_prompts()
    payload = json.loads(lib_path.read_text(encoding="utf-8"))
    assert payload.get("version") == 1
