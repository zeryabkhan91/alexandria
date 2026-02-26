from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src import intelligent_prompter as ip


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text or json.dumps(payload)

    def json(self):  # type: ignore[no-untyped-def]
        return self._payload


def _book_row(number: int = 1) -> dict:
    return {
        "number": number,
        "title": "Moby Dick",
        "author": "Herman Melville",
        "folder_name": f"{number}. Moby Dick",
        "file_base": "Moby Dick",
        "enrichment": {
            "genre": "Adventure",
            "protagonist": "Captain Ahab",
            "setting_primary": "Sea",
            "emotional_tone": "obsession and fate",
            "iconic_scenes": ["Ahab sights the whale", "Storm pursuit", "Final harpoon"],
            "visual_motifs": ["harpoon", "storm waves", "ship mast"],
            "symbolic_elements": ["white whale", "broken mast"],
            "key_characters": ["Ahab", "Ishmael", "Starbuck"],
        },
    }


def _quality_history_payload(tmp_path: Path) -> tuple[Path, Path]:
    quality = tmp_path / "quality_scores.json"
    history = tmp_path / "generation_history.json"
    quality.write_text(
        json.dumps(
            {
                "scores": [
                    {"book_number": 1, "variant_id": 1, "overall_score": 0.91},
                    {"book_number": 1, "variant_id": 2, "overall_score": 0.77},
                ]
            }
        ),
        encoding="utf-8",
    )
    history.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "book_number": 1,
                        "variant": 1,
                        "prompt": "symbolic sepia portrait of Captain Ahab, circular vignette composition, no text, no letters, no words, no watermarks",
                        "quality_score": 0.91,
                        "book_title": "Moby Dick",
                        "model": "m1",
                    },
                    {
                        "book_number": 1,
                        "variant": 2,
                        "prompt": "generic scene near the sea",
                        "quality_score": 0.55,
                        "book_title": "Moby Dick",
                        "model": "m2",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    return quality, history


def test_helper_functions_and_constraints():
    assert ip._genre_key("gothic horror") == "gothic_horror"
    assert ip._genre_key("") == "literary_fiction"
    assert ip._tokenize("Ahab's storm!") == ["ahab's", "storm"]
    assert ip._token_jaccard("a b c", "b c d") > 0
    assert ip._clip(-1.0) == 0.0
    assert ip._clip(3.0) == 1.0
    assert ip._parse_json("```json\n{\"a\":1}\n```")["a"] == 1
    assert ip._parse_books("1,3-4") == [1, 3, 4]
    assert ip._safe_int("x", 9) == 9
    assert ip._safe_float("x", 1.5) == 1.5

    constrained = ip._ensure_prompt_constraints("short prompt")
    low = constrained.lower()
    assert ip.REQUIRED_COMPOSITION in low
    assert ip.REQUIRED_TEXT_BLOCK in low
    assert 40 <= len(constrained.split()) <= 80


def test_llm_generate_and_regenerate_fallback_and_calls(monkeypatch):
    row = _book_row()
    runtime = SimpleNamespace(anthropic_api_key="", openai_api_key="")

    fallback = ip._llm_generate_variant_prompts(
        row=row,
        provider="openai",
        model="gpt-4o",
        max_tokens=200,
        runtime=runtime,
        genre_presets={},
        top_patterns=[],
    )
    assert len(fallback) == 5

    runtime2 = SimpleNamespace(anthropic_api_key="k", openai_api_key="k")
    monkeypatch.setattr(
        ip,
        "_call_llm_json",
        lambda **_kwargs: json.dumps({"prompts": ["p1", "p2", "p3", "p4", "p5"]}),
    )
    generated = ip._llm_generate_variant_prompts(
        row=row,
        provider="openai",
        model="gpt-4o",
        max_tokens=200,
        runtime=runtime2,
        genre_presets={},
        top_patterns=[],
    )
    assert len(generated) == 5

    monkeypatch.setattr(ip, "_call_llm_json", lambda **_kwargs: json.dumps({"prompt": "rewritten prompt"}))
    quality = ip.PromptQuality(0.2, 0.2, 0.2, 0.2, 0.2)
    rewritten = ip._llm_regenerate_single(
        row=row,
        current="old prompt",
        quality=quality,
        variant_name="ICONIC SCENE",
        provider="openai",
        model="gpt-4o",
        max_tokens=200,
        runtime=runtime2,
        genre_presets={},
        top_patterns=[],
        attempt=1,
    )
    assert "rewritten" in rewritten


def test_call_llm_json_paths(monkeypatch):
    def _fake_post(_url, headers=None, json=None, timeout=None):  # type: ignore[no-untyped-def]
        if "x-api-key" in (headers or {}):
            return _FakeResponse(200, {"content": [{"type": "text", "text": "{\"ok\":true}"}]})
        return _FakeResponse(200, {"choices": [{"message": {"content": "{\"ok\":true}"}}]})

    monkeypatch.setattr(ip.requests, "post", _fake_post)
    text_a = ip._call_llm_json(
        provider="anthropic",
        api_key="k",
        model="m",
        max_tokens=50,
        system_prompt="s",
        user_prompt="u",
    )
    assert "ok" in text_a

    text_o = ip._call_llm_json(
        provider="openai",
        api_key="k",
        model="m",
        max_tokens=50,
        system_prompt="s",
        user_prompt="u",
    )
    assert "ok" in text_o

    with pytest.raises(RuntimeError):
        ip._call_llm_json(
            provider="bad",
            api_key="k",
            model="m",
            max_tokens=50,
            system_prompt="s",
            user_prompt="u",
        )


def test_score_and_feedback_update(tmp_path: Path):
    row = _book_row()
    p1 = "Captain Ahab portrait in sepia with crosshatching and chiaroscuro, circular vignette composition, no text, no letters, no words, no watermarks"
    p2 = "generic scene, circular vignette composition, no text, no letters, no words, no watermarks"
    quality = ip._score_prompt(p1, row=row, peers=[p2])
    assert 0.0 <= quality.overall <= 1.0
    assert quality.specificity >= 0.0

    quality_path, history_path = _quality_history_payload(tmp_path)
    performance_path = tmp_path / "prompt_performance.json"

    class FakeLibrary:
        def __init__(self):
            self.saved = []

        def save_prompt(self, prompt):  # type: ignore[no-untyped-def]
            self.saved.append(prompt)

    lib = FakeLibrary()
    perf = ip.update_prompt_feedback(
        quality_scores_path=quality_path,
        generation_history_path=history_path,
        prompt_output_path=tmp_path / "prompts.json",
        performance_path=performance_path,
        prompt_library=lib,  # type: ignore[arg-type]
    )
    assert any(bucket["count"] >= 1 for bucket in perf["patterns"].values())
    assert perf["patterns"]["symbolic_with_color_direction"]["count"] >= 1
    assert performance_path.exists()
    assert len(lib.saved) >= 1


def test_generate_prompts_and_main(tmp_path: Path, monkeypatch):
    catalog_path = tmp_path / "book_catalog_enriched.json"
    output_path = tmp_path / "book_prompts_intelligent.json"
    presets_path = tmp_path / "genre_presets.json"
    perf_path = tmp_path / "prompt_performance.json"
    library_path = tmp_path / "prompt_library.json"
    templates_path = tmp_path / "prompt_templates.json"

    catalog_path.write_text(json.dumps([_book_row(1), _book_row(2)]), encoding="utf-8")
    presets_path.write_text(json.dumps({"adventure": {"style": "classic"}}), encoding="utf-8")
    perf_path.write_text(json.dumps({"patterns": {"specific_character_action": {"avg_score": 0.9}}}), encoding="utf-8")
    library_path.write_text(json.dumps({"prompts": [], "mixes": []}), encoding="utf-8")
    templates_path.write_text(json.dumps({"negative_prompt": "text, letters"}), encoding="utf-8")

    runtime = SimpleNamespace(
        llm_provider="openai",
        llm_model="gpt-4o",
        llm_max_tokens=1000,
        prompt_templates_path=templates_path,
        prompt_library_path=library_path,
        anthropic_api_key="",
        openai_api_key="",
    )
    monkeypatch.setattr(ip.config, "get_config", lambda: runtime)
    monkeypatch.setattr(
        ip,
        "_generate_prompts_for_book",
        lambda **kwargs: [
            {
                "variant_id": 1,
                "variant_key": "1_iconic_scene_sketch",
                "variant_name": "ICONIC SCENE",
                "description": "x",
                "prompt": ip._ensure_prompt_constraints("A detailed classical scene"),
                "negative_prompt": "text",
                "style_reference": "intelligent_llm",
                "word_count": 45,
                "quality": {"overall": 0.9},
            }
        ],
    )
    monkeypatch.setattr(ip, "update_prompt_feedback", lambda **_kwargs: {"ok": True})

    summary = ip.generate_prompts(
        catalog_path=catalog_path,
        output_path=output_path,
        books=[1],
        count=1,
        provider="openai",
        model="gpt-4o",
        max_tokens=1000,
        genre_presets_path=presets_path,
        performance_path=perf_path,
        prompt_library_path=library_path,
    )
    assert summary["books_total"] == 2
    assert summary["books_generated_in_run"] == 1
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["book_count"] == 2

    args = SimpleNamespace(
        catalog=catalog_path,
        output=output_path,
        books="1",
        count=1,
        provider=None,
        model=None,
        max_tokens=None,
        genre_presets=presets_path,
        performance=perf_path,
    )
    monkeypatch.setattr(ip.argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(ip, "generate_prompts", lambda **_kwargs: {"ok": True})
    assert ip.main() == 0


def test_generate_prompts_selected_books_preserves_prior_record(tmp_path: Path, monkeypatch):
    catalog_path = tmp_path / "catalog.json"
    output_path = tmp_path / "prompts.json"
    templates_path = tmp_path / "prompt_templates.json"
    library_path = tmp_path / "prompt_library.json"
    perf_path = tmp_path / "perf.json"
    presets_path = tmp_path / "genre.json"

    catalog_path.write_text(json.dumps([_book_row(1), _book_row(2)]), encoding="utf-8")
    output_path.write_text(json.dumps({"books": [{"number": 2, "variants": [{"prompt": "prior"}]}]}), encoding="utf-8")
    templates_path.write_text(json.dumps({"negative_prompt": "text"}), encoding="utf-8")
    library_path.write_text(json.dumps({"prompts": [], "mixes": []}), encoding="utf-8")
    perf_path.write_text(json.dumps({}), encoding="utf-8")
    presets_path.write_text(json.dumps({}), encoding="utf-8")

    runtime = SimpleNamespace(
        llm_provider="openai",
        llm_model="gpt-4o",
        llm_max_tokens=400,
        prompt_templates_path=templates_path,
        prompt_library_path=library_path,
        anthropic_api_key="",
        openai_api_key="",
    )
    monkeypatch.setattr(ip.config, "get_config", lambda: runtime)
    monkeypatch.setattr(
        ip,
        "_generate_prompts_for_book",
        lambda **_kwargs: [
            {
                "variant_id": 1,
                "variant_key": "1_iconic_scene_sketch",
                "variant_name": "ICONIC SCENE",
                "description": "x",
                "prompt": ip._ensure_prompt_constraints("A detailed classical scene"),
                "negative_prompt": "text",
                "style_reference": "intelligent_llm",
                "word_count": 45,
                "quality": {"overall": 0.9},
            }
        ],
    )
    monkeypatch.setattr(ip, "update_prompt_feedback", lambda **_kwargs: {"ok": True})

    summary = ip.generate_prompts(
        catalog_path=catalog_path,
        output_path=output_path,
        books=[1],
        count=1,
        genre_presets_path=presets_path,
        performance_path=perf_path,
        prompt_library_path=library_path,
    )
    assert summary["books_total"] == 2
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    book2 = [row for row in payload["books"] if int(row.get("number", 0)) == 2][0]
    assert book2["variants"][0]["prompt"] == "prior"


def test_generate_prompts_writes_output_and_feedback_in_single_staged_write(tmp_path: Path, monkeypatch):
    catalog_path = tmp_path / "catalog.json"
    output_path = tmp_path / "prompts.json"
    templates_path = tmp_path / "prompt_templates.json"
    library_path = tmp_path / "prompt_library.json"
    perf_path = tmp_path / "perf.json"
    presets_path = tmp_path / "genre.json"

    catalog_path.write_text(json.dumps([_book_row(1)]), encoding="utf-8")
    templates_path.write_text(json.dumps({"negative_prompt": "text"}), encoding="utf-8")
    library_path.write_text(json.dumps({"prompts": [], "mixes": []}), encoding="utf-8")
    perf_path.write_text(json.dumps({}), encoding="utf-8")
    presets_path.write_text(json.dumps({}), encoding="utf-8")

    runtime = SimpleNamespace(
        llm_provider="openai",
        llm_model="gpt-4o",
        llm_max_tokens=400,
        prompt_templates_path=templates_path,
        prompt_library_path=library_path,
        anthropic_api_key="",
        openai_api_key="",
    )
    monkeypatch.setattr(ip.config, "get_config", lambda: runtime)
    monkeypatch.setattr(
        ip,
        "_generate_prompts_for_book",
        lambda **_kwargs: [
            {
                "variant_id": 1,
                "variant_key": "1_iconic_scene_sketch",
                "variant_name": "ICONIC SCENE",
                "description": "x",
                "prompt": ip._ensure_prompt_constraints("A detailed classical scene"),
                "negative_prompt": "text",
                "style_reference": "intelligent_llm",
                "word_count": 45,
                "quality": {"overall": 0.9},
            }
        ],
    )
    monkeypatch.setattr(ip, "update_prompt_feedback", lambda **_kwargs: {"updated_at": "now", "patterns": {}, "auto_saved_prompts": 0})

    writes: list[list[Path]] = []

    def _fake_atomic_many(items):  # type: ignore[no-untyped-def]
        writes.append([path for path, _ in items])
        for path, payload in items:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    monkeypatch.setattr(ip.safe_json, "atomic_write_many_json", _fake_atomic_many)

    ip.generate_prompts(
        catalog_path=catalog_path,
        output_path=output_path,
        books=[1],
        count=1,
        genre_presets_path=presets_path,
        performance_path=perf_path,
        prompt_library_path=library_path,
    )

    assert len(writes) == 1
    assert writes[0] == [output_path, perf_path]
    assert output_path.exists()
    assert perf_path.exists()


def test_generate_prompts_for_book_wrapper_and_negative_prompt_fallback(tmp_path: Path, monkeypatch):
    runtime = SimpleNamespace(
        llm_provider="openai",
        llm_model="gpt-4o",
        llm_max_tokens=200,
        prompt_templates_path=tmp_path / "templates.json",
        anthropic_api_key="",
        openai_api_key="",
    )
    runtime.prompt_templates_path.write_text("{bad", encoding="utf-8")
    monkeypatch.setattr(ip.config, "get_config", lambda: runtime)
    monkeypatch.setattr(ip, "_generate_prompts_for_book", lambda **_kwargs: [{"prompt": "ok"}])
    rows = ip.generate_prompts_for_book(book=_book_row(1), runtime=runtime)
    assert rows == [{"prompt": "ok"}]
    assert "watermark" in ip._default_negative_prompt(runtime.prompt_templates_path)


def test_generate_prompts_core_quality_loop_and_fallback(monkeypatch):
    row = _book_row()
    runtime = SimpleNamespace(anthropic_api_key="k", openai_api_key="k")

    monkeypatch.setattr(ip, "_llm_generate_variant_prompts", lambda **_kwargs: ["p1"])
    monkeypatch.setattr(
        ip,
        "_score_prompt",
        lambda prompt, **_kwargs: ip.PromptQuality(0.1, 0.1, 0.1, 0.1, 0.1 if "fallback" not in prompt else 0.9),
    )
    monkeypatch.setattr(ip, "_llm_regenerate_single", lambda **_kwargs: "")

    variants = ip._generate_prompts_for_book(
        row=row,
        count=2,
        provider="openai",
        model="gpt-4o",
        max_tokens=100,
        runtime=runtime,
        genre_presets={},
        top_patterns=[],
        negative_prompt="text",
    )
    assert len(variants) == 2
    assert all("prompt" in item for item in variants)


def test_llm_generate_variant_prompts_parsing_and_fallback_paths(monkeypatch):
    row = _book_row()
    runtime = SimpleNamespace(openai_api_key="k", anthropic_api_key="")

    # Dict entries path.
    monkeypatch.setattr(
        ip,
        "_call_llm_json",
        lambda **_kwargs: json.dumps(
            {
                "prompts": [
                    {"prompt": "one"},
                    {"prompt": "two"},
                    {"prompt": "three"},
                    {"prompt": "four"},
                    {"prompt": "five"},
                ]
            }
        ),
    )
    prompts = ip._llm_generate_variant_prompts(
        row=row,
        provider="openai",
        model="gpt-4o",
        max_tokens=100,
        runtime=runtime,
        genre_presets={},
        top_patterns=[],
    )
    assert len(prompts) == 5

    # Too-short prompt list should fallback.
    monkeypatch.setattr(ip, "_call_llm_json", lambda **_kwargs: json.dumps({"prompts": ["only-one"]}))
    prompts2 = ip._llm_generate_variant_prompts(
        row=row,
        provider="openai",
        model="gpt-4o",
        max_tokens=100,
        runtime=runtime,
        genre_presets={},
        top_patterns=[],
    )
    assert len(prompts2) == 5


def test_llm_regenerate_single_no_key_and_rewritten_key(monkeypatch):
    row = _book_row()
    quality = ip.PromptQuality(0.1, 0.1, 0.1, 0.1, 0.1)
    runtime_no_key = SimpleNamespace(openai_api_key="")
    assert (
        ip._llm_regenerate_single(
            row=row,
            current="x",
            quality=quality,
            variant_name="ICONIC SCENE",
            provider="openai",
            model="gpt-4o",
            max_tokens=100,
            runtime=runtime_no_key,
            genre_presets={},
            top_patterns=[],
            attempt=1,
        )
        == ""
    )

    runtime_with_key = SimpleNamespace(openai_api_key="k")
    monkeypatch.setattr(ip, "_call_llm_json", lambda **_kwargs: json.dumps({"rewritten_prompt": "rewritten"}))
    rewritten = ip._llm_regenerate_single(
        row=row,
        current="x",
        quality=quality,
        variant_name="ICONIC SCENE",
        provider="openai",
        model="gpt-4o",
        max_tokens=100,
        runtime=runtime_with_key,
        genre_presets={},
        top_patterns=[],
        attempt=1,
    )
    assert rewritten == "rewritten"

    monkeypatch.setattr(ip, "_call_llm_json", lambda **_kwargs: json.dumps({"other": "x"}))
    assert (
        ip._llm_regenerate_single(
            row=row,
            current="x",
            quality=quality,
            variant_name="ICONIC SCENE",
            provider="openai",
            model="gpt-4o",
            max_tokens=100,
            runtime=runtime_with_key,
            genre_presets={},
            top_patterns=[],
            attempt=1,
        )
        == ""
    )


def test_call_llm_json_error_and_empty_choice_paths(monkeypatch):
    # Anthropic error path.
    monkeypatch.setattr(ip.requests, "post", lambda *_args, **_kwargs: _FakeResponse(500, {}, "err"))
    with pytest.raises(RuntimeError):
        ip._call_llm_json(
            provider="anthropic",
            api_key="k",
            model="m",
            max_tokens=10,
            system_prompt="s",
            user_prompt="u",
        )

    # OpenAI error path.
    with pytest.raises(RuntimeError):
        ip._call_llm_json(
            provider="openai",
            api_key="k",
            model="m",
            max_tokens=10,
            system_prompt="s",
            user_prompt="u",
        )

    # OpenAI empty choices path.
    monkeypatch.setattr(ip.requests, "post", lambda *_args, **_kwargs: _FakeResponse(200, {"choices": []}))
    assert (
        ip._call_llm_json(
            provider="openai",
            api_key="k",
            model="m",
            max_tokens=10,
            system_prompt="s",
            user_prompt="u",
        )
        == ""
    )


def test_update_feedback_skip_paths_templateization_and_save_errors(tmp_path: Path, monkeypatch):
    quality_path = tmp_path / "quality.json"
    history_path = tmp_path / "history.json"
    output_path = tmp_path / "output.json"
    performance_path = tmp_path / "perf.json"

    quality_path.write_text(
        json.dumps(
            {
                "scores": [
                    {"book_number": 1, "variant_id": 1, "overall_score": 0.95},
                    {"book_number": 2, "variant_id": 1, "overall_score": 0.0},
                ]
            }
        ),
        encoding="utf-8",
    )
    history_path.write_text(
        json.dumps(
            {
                "items": [
                    "bad-row",
                    {"book_number": 1, "variant": 1, "prompt": "captain portrait", "book_title": "Moby Dick", "model": "m"},
                    {"book_number": 2, "variant": 1, "prompt": "generic", "quality_score": 0.0, "model": "m"},
                    {"book_number": 3, "variant": 1, "prompt": "", "quality_score": 0.9, "model": "m"},
                ]
            }
        ),
        encoding="utf-8",
    )

    class _BadLibrary:
        def save_prompt(self, _prompt):  # type: ignore[no-untyped-def]
            raise RuntimeError("duplicate")

    monkeypatch.setattr(ip, "_templateize_prompt", lambda *_args, **_kwargs: "plain prompt")
    perf = ip.update_prompt_feedback(
        quality_scores_path=quality_path,
        generation_history_path=history_path,
        prompt_output_path=output_path,
        performance_path=performance_path,
        prompt_library=_BadLibrary(),  # type: ignore[arg-type]
    )
    assert perf["patterns"]["generic_scene_description"]["count"] >= 0


def test_misc_helpers_and_parsers_edge_paths(tmp_path: Path):
    assert ip._classify_pattern("captain ahab portrait") == "specific_character_action"
    assert ip._templateize_prompt("", title_hint="x") == ""
    assert ip._templateize_prompt("prompt with {title}", title_hint="Title") == "prompt with {title}"
    assert ip._templateize_prompt("Moby Dick at sea", title_hint="Moby Dick") == "{title} at sea"
    assert ip._variant_description(row={"enrichment": {}}, index=0) == "Book-specific intelligent variant 1"

    compliance_drop = ip._score_prompt("very short prompt without required constraints", row=_book_row(), peers=[])
    assert compliance_drop.constraint_compliance < 1.0
    assert ip._ensure_prompt_constraints("").lower().startswith("classical illustration")
    long = "word " * 120
    assert ip._word_count(ip._ensure_prompt_constraints(long)) <= 80

    assert ip._genre_key("mystery") == "literary_fiction"
    perf_path = tmp_path / "perf.json"
    perf_path.write_text(json.dumps({"patterns": {"good": {"avg_score": 0.9}, "bad": "x"}}), encoding="utf-8")
    assert ip._top_patterns(perf_path)[0] == "good"
    assert ip._token_jaccard("", "") == 0.0

    assert ip._parse_books(None) is None
    assert ip._parse_books("1, ,2-3,bad") == [1, 2, 3]
    assert ip._parse_json("") == {}
    assert ip._parse_json("no-json here") == {}
    assert ip._parse_json("wrapper {bad} text") == {}

    dict_path = tmp_path / "dict.json"
    list_path = tmp_path / "list.json"
    assert ip._load_json_dict(dict_path) == {}
    assert ip._load_json_list(list_path) == []
    dict_path.write_text("{bad", encoding="utf-8")
    list_path.write_text("{bad", encoding="utf-8")
    assert ip._load_json_dict(dict_path) == {}
    assert ip._load_json_list(list_path) == []
    list_path.write_text(json.dumps({"x": 1}), encoding="utf-8")
    assert ip._load_json_list(list_path) == []
