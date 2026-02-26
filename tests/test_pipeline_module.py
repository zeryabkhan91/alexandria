from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src import pipeline as pl


def _touch(path: Path, data: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data, encoding="utf-8")


def _catalog(path: Path) -> list[dict]:
    rows = [
        {"number": 1, "title": "Book One", "author": "Author One", "folder_name": "1. Book One", "file_base": "Book One"},
        {"number": 2, "title": "Book Two", "author": "Author Two", "folder_name": "2. Book Two copy", "file_base": "Book Two"},
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows), encoding="utf-8")
    return rows


def _prompts(path: Path) -> None:
    payload = {
        "negative_prompt": "bad",
        "books": [
            {
                "number": 1,
                "title": "Book One",
                "variants": [
                    {"variant_id": 1, "prompt": "P1", "negative_prompt": "N1"},
                    {"variant_id": 2, "prompt": "P2", "negative_prompt": "N2"},
                ],
            }
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _runtime(tmp_path: Path) -> SimpleNamespace:
    config_dir = tmp_path / "config"
    data_dir = tmp_path / "data"
    tmp_dir = tmp_path / "tmp"
    output_dir = tmp_path / "Output Covers"
    input_dir = tmp_path / "Input Covers"
    for p in [config_dir, data_dir, tmp_dir, output_dir, input_dir]:
        p.mkdir(parents=True, exist_ok=True)

    catalog_path = config_dir / "book_catalog.json"
    _catalog(catalog_path)
    prompts_path = config_dir / "book_prompts.json"
    _prompts(prompts_path)
    (config_dir / "cover_regions.json").write_text(json.dumps({"covers": []}), encoding="utf-8")
    (config_dir / "prompt_templates.json").write_text(json.dumps({"negative_prompt": "bad"}), encoding="utf-8")
    (config_dir / "prompt_library.json").write_text(json.dumps({"prompts": [], "mixes": []}), encoding="utf-8")
    (config_dir / "model_prompt_overrides.json").write_text(json.dumps({"models": {}}), encoding="utf-8")

    runtime = SimpleNamespace(
        project_root=tmp_path,
        catalog_id="classics",
        input_dir=input_dir,
        output_dir=output_dir,
        tmp_dir=tmp_dir,
        data_dir=data_dir,
        config_dir=config_dir,
        book_catalog_path=catalog_path,
        prompts_path=prompts_path,
        prompt_templates_path=config_dir / "prompt_templates.json",
        prompt_library_path=config_dir / "prompt_library.json",
        model_prompt_overrides_path=config_dir / "model_prompt_overrides.json",
        generation_state_path=data_dir / "generation_state.json",
        ai_model="openai/gpt-image-1",
        all_models=["openai/gpt-image-1", "openrouter/flux-2-pro"],
        variants_per_cover=2,
        max_retries=2,
        min_quality_score=0.6,
        cover_style="navy_gold_medallion",
        llm_provider="openai",
        llm_model="gpt-4o",
        llm_max_tokens=800,
        gdrive_output_folder_id=f"local:{(tmp_path / 'drive-mirror')}",
        google_credentials_path="",
        failures_path=data_dir / "generation_failures.json",
        webhook_url="",
        webhook_events=[],
    )
    runtime.get_api_key = lambda provider: "key" if provider in {"openai", "openrouter", "google", "replicate", "fal"} else ""  # type: ignore[attr-defined]
    runtime.resolve_model_provider = (  # type: ignore[attr-defined]
        lambda model: "openai" if "openai" in model else "openrouter"
    )
    runtime.get_model_cost = lambda _model: 0.05  # type: ignore[attr-defined]
    return runtime


def _book_result(book_number: int, status: str = "success") -> pl.BookRunResult:
    return pl.BookRunResult(
        book_number=book_number,
        status=status,
        generated=2,
        quality_passed=2,
        composited=2,
        exported=6,
        duration_seconds=1.2,
        cost_usd=0.1,
        error=None if status == "success" else "boom",
    )


def test_parse_and_small_helpers(tmp_path: Path):
    runtime = _runtime(tmp_path)
    assert pl._normalize_providers(None) == pl.SUPPORTED_PROVIDERS
    assert pl._normalize_providers([" OPENAI ", "openai", "google"]) == ["openai", "google"]
    assert pl._parse_books("1,3-4") == [1, 3, 4]
    assert pl._resolve_models({"all_models": True}, runtime) == runtime.all_models
    assert pl._resolve_models({"models": "a,b"}, runtime) == ["a", "b"]
    assert pl._resolve_models({"model": "a"}, runtime) == ["a"]
    assert pl._resolve_models({}, runtime) is None
    assert pl._resolve_variant_options("3", None, runtime) == (3, [])
    assert pl._resolve_variant_options("1-2", None, runtime) == (1, [1, 2])
    assert pl._resolve_variant_options(None, 4, runtime) == (1, [4])
    assert "OPENAI" in pl._format_api_key_report({"providers": [{"provider": "openai", "status": "KEY_VALID", "detail": "HTTP 200"}]})
    assert pl._resolve_model_prompt(
        model="openai/gpt-image-1",
        title="Title",
        fallback_prompt="fallback",
        overrides={"openai/gpt-image-1": {"prompt_template": "Prompt for {title}"}},
        explicit_prompt_requested=False,
    ) == "Prompt for Title"
    assert pl._resolve_model_prompt(
        model="openai/gpt-image-1",
        title="Title",
        fallback_prompt="fallback",
        overrides={"openai/gpt-image-1": {"prompt_template": "Prompt for {missing}"}},
        explicit_prompt_requested=False,
    ) == "Prompt for {missing}"


def test_signal_helpers(monkeypatch):
    events = []
    monkeypatch.setattr(pl.signal, "signal", lambda sig, fn: events.append((sig, fn)))
    pl._install_signal_handlers()
    assert len(events) == 2
    pl._request_shutdown(15, None)
    assert pl._SHUTDOWN_REQUESTED is True


def test_state_and_summary_helpers(tmp_path: Path, monkeypatch):
    runtime = _runtime(tmp_path)
    monkeypatch.setattr(pl, "PIPELINE_STATE_PATH", tmp_path / "data" / "pipeline_state.json")
    monkeypatch.setattr(pl, "PIPELINE_SUMMARY_PATH", tmp_path / "data" / "pipeline_summary.json")
    monkeypatch.setattr(pl, "PIPELINE_SUMMARY_MD_PATH", tmp_path / "data" / "pipeline_summary.md")

    state = pl._load_pipeline_state(runtime=runtime)
    assert state["catalog"] == "classics"

    pl._save_pipeline_state({"completed_books": {"1": {"status": "success"}}}, runtime=runtime)
    loaded = pl._load_pipeline_state(runtime=runtime)
    assert "1" in loaded["completed_books"]

    # Invalid JSON and catalog mismatch both reset state.
    pl.PIPELINE_STATE_PATH.write_text("{bad-json", encoding="utf-8")
    bad = pl._load_pipeline_state(runtime=runtime)
    assert bad["catalog"] == runtime.catalog_id
    pl.PIPELINE_STATE_PATH.write_text(
        json.dumps({"catalog": "other", "completed_books": {"1": {}}, "failed_books": {}}),
        encoding="utf-8",
    )
    mismatch = pl._load_pipeline_state(runtime=runtime)
    assert mismatch["completed_books"] == {}

    # Complete book needs at least 15 files.
    state = {"completed_books": {"1": {"status": "success"}}, "failed_books": {}}
    book_dir = runtime.output_dir / "1. Book One"
    for idx in range(15):
        _touch(book_dir / f"f{idx}.jpg")
    assert pl._book_is_complete(1, runtime.output_dir, state, runtime.book_catalog_path) is True

    summary = pl.PipelineResult(
        processed_books=1,
        succeeded_books=1,
        failed_books=0,
        skipped_books=0,
        generated_images=2,
        exported_files=6,
        dry_run=False,
        interrupted=False,
        started_at=pl._utc_now(),
        finished_at=pl._utc_now(),
        book_results=[_book_result(1)],
    )
    pl._write_summary(summary)
    assert pl.PIPELINE_SUMMARY_PATH.exists()
    assert pl.PIPELINE_SUMMARY_MD_PATH.exists()


def test_state_and_summary_helpers_use_catalog_scoped_paths_for_non_classics(tmp_path: Path):
    runtime = _runtime(tmp_path)
    runtime.catalog_id = "demo"

    pl._save_pipeline_state({"completed_books": {"1": {"status": "success"}}, "failed_books": {}}, runtime=runtime)
    state_path = runtime.data_dir / "pipeline_state_demo.json"
    assert state_path.exists()
    loaded = json.loads(state_path.read_text(encoding="utf-8"))
    assert loaded["catalog"] == "demo"

    summary = pl.PipelineResult(
        processed_books=1,
        succeeded_books=1,
        failed_books=0,
        skipped_books=0,
        generated_images=2,
        exported_files=6,
        dry_run=False,
        interrupted=False,
        started_at=pl._utc_now(),
        finished_at=pl._utc_now(),
        book_results=[_book_result(1)],
    )
    pl._write_summary(summary, runtime=runtime)
    assert (runtime.data_dir / "pipeline_summary_demo.json").exists()
    assert (runtime.data_dir / "pipeline_summary_demo.md").exists()


def test_quality_threshold_prioritization_and_estimate(tmp_path: Path):
    runtime = _runtime(tmp_path)
    quality_path = runtime.data_dir / "quality_scores.json"
    quality_path.write_text(
        json.dumps(
            {
                "scores": [
                    {"book_number": 1, "overall_score": 0.4},
                    {"book_number": 2, "overall_score": 0.9},
                ]
            }
        ),
        encoding="utf-8",
    )

    low = pl._books_below_quality_threshold(runtime=runtime, threshold=0.6)
    assert low == {1}

    state = {"completed_books": {}, "failed_books": {"2": {"error": "x"}}}
    ordered = pl._prioritize_books(
        [1, 2, 3],
        output_dir=runtime.output_dir,
        runtime=runtime,
        priority_order="high,medium,low",
        state=state,
    )
    assert ordered[0] in {1, 2}

    estimate = pl.estimate_batch(runtime=runtime, books=[1, 2], models=["openai/gpt-image-1"], variants_per_model=2, workers=2)
    assert estimate["books"] == 2
    assert estimate["estimated_cost"] > 0


def test_probe_provider_key_and_api_tests(tmp_path: Path, monkeypatch):
    runtime = _runtime(tmp_path)

    def _fake_get(url, headers=None, timeout=None):  # type: ignore[no-untyped-def]
        if "openai.com" in url:
            return SimpleNamespace(status_code=200, text="ok")
        return SimpleNamespace(status_code=401, text="denied")

    monkeypatch.setattr(pl.requests, "get", _fake_get)
    ok, detail = pl._probe_provider_key(provider="openai", api_key="k", timeout=2.0)
    assert ok is True and "HTTP 200" in detail

    bad, detail2 = pl._probe_provider_key(provider="google", api_key="k", timeout=2.0)
    assert bad is False
    assert "HTTP 401" in detail2

    report = pl.test_api_keys(runtime=runtime, providers=["openai", "google"], timeout=2.0)
    assert len(report["providers"]) == 2


def test_prepare_prompt_source_and_prerequisites(tmp_path: Path, monkeypatch):
    runtime = _runtime(tmp_path)
    overrides = {"intelligent_prompts": True}

    monkeypatch.setattr(pl.book_enricher, "enrich_catalog", lambda **_kwargs: {"ok": True})
    monkeypatch.setattr(pl.intelligent_prompter, "generate_prompts", lambda **_kwargs: {"ok": True})
    target = pl._prepare_prompt_source(runtime=runtime, config_overrides=overrides)
    assert target == runtime.config_dir / "book_prompts_intelligent.json"

    # Fallback to legacy when intelligent generation fails.
    monkeypatch.setattr(pl.intelligent_prompter, "generate_prompts", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("fail")))
    assert pl._prepare_prompt_source(runtime=runtime, config_overrides=overrides) is None

    # Ensure prerequisites creates missing artifacts.
    (runtime.config_dir / "cover_regions.json").unlink(missing_ok=True)
    runtime.prompts_path.unlink(missing_ok=True)
    runtime.prompt_library_path.unlink(missing_ok=True)

    called = {"analyze": 0, "gen": 0, "save": 0, "lib": 0}
    monkeypatch.setattr(pl.cover_analyzer, "analyze_all_covers", lambda *_args, **_kwargs: called.__setitem__("analyze", called["analyze"] + 1))
    monkeypatch.setattr(pl.prompt_generator, "generate_all_prompts", lambda **_kwargs: (called.__setitem__("gen", called["gen"] + 1) or [{"number": 1}]))
    monkeypatch.setattr(pl.prompt_generator, "save_prompts", lambda *_args, **_kwargs: called.__setitem__("save", called["save"] + 1))

    class _PromptLib:
        def __init__(self, _path):
            called["lib"] += 1

    monkeypatch.setattr(pl, "PromptLibrary", _PromptLib)
    pl._ensure_prerequisites(input_dir=runtime.input_dir, runtime=runtime, prompts_path=None)
    assert called["analyze"] == 1
    assert called["gen"] == 1
    assert called["save"] == 1
    assert called["lib"] == 1


def test_generate_with_model_prompts_and_run_single_book(tmp_path: Path, monkeypatch):
    runtime = _runtime(tmp_path)
    generated_dir = runtime.tmp_dir / "generated" / "1"
    generated_dir.mkdir(parents=True, exist_ok=True)
    _touch(generated_dir / "variant_1.png")

    calls = []
    monkeypatch.setattr(
        pl.image_generator,
        "generate_all_models",
        lambda **kwargs: (calls.append(kwargs) or [_book_result(1)]),  # type: ignore[list-item]
    )
    out = pl._generate_with_model_prompts(
        book_number=1,
        base_prompt="base",
        negative_prompt="neg",
        models=["openai/gpt-image-1", "openrouter/flux-2-pro"],
        variants_per_model=1,
        title="Book One",
        overrides={},
        explicit_prompt_requested=False,
        output_dir=runtime.tmp_dir / "generated",
        resume=True,
        dry_run=True,
        provider_override=None,
    )
    assert out
    assert len(calls) == 1

    # Dry-run single book path.
    monkeypatch.setattr(pl.image_generator, "GenerationResult", SimpleNamespace)  # type: ignore[arg-type]
    monkeypatch.setattr(
        pl,
        "_generate_with_model_prompts",
        lambda **_kwargs: [
            SimpleNamespace(success=True, cost=0.1),
            SimpleNamespace(success=True, cost=0.1),
        ],
    )
    result = pl._run_single_book(
        book_number=1,
        runtime=runtime,
        input_dir=runtime.input_dir,
        output_dir=runtime.output_dir,
        prompts_path_override=None,
        model_list=["openai/gpt-image-1"],
        dry_run=True,
        variation_count=2,
        prompt_variant_ids=[],
        prompt_override=None,
        use_library=False,
        prompt_id=None,
        style_anchors=[],
        all_models=False,
        provider=None,
        no_resume=False,
    )
    assert result.status == "success"
    assert result.generated == 2


def test_run_single_book_full_path(tmp_path: Path, monkeypatch):
    runtime = _runtime(tmp_path)
    (runtime.config_dir / "cover_regions.json").write_text(
        json.dumps({"covers": [{"cover_id": 1, "center_x": 1, "center_y": 1, "radius": 1, "frame_bbox": [0, 0, 1, 1]}]}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        pl,
        "_generate_with_model_prompts",
        lambda **_kwargs: [SimpleNamespace(success=True, cost=0.2)],
    )
    monkeypatch.setattr(
        pl.quality_gate,
        "run_quality_gate",
        lambda **_kwargs: [SimpleNamespace(book_number=1, passed=True), SimpleNamespace(book_number=2, passed=False)],
    )
    monkeypatch.setattr(pl.cover_compositor, "composite_all_variants", lambda **_kwargs: [Path("a"), Path("b")])
    monkeypatch.setattr(pl.output_exporter, "export_book_variants", lambda **_kwargs: [Path("a"), Path("b"), Path("c")])

    result = pl._run_single_book(
        book_number=1,
        runtime=runtime,
        input_dir=runtime.input_dir,
        output_dir=runtime.output_dir,
        prompts_path_override=None,
        model_list=["openai/gpt-image-1"],
        dry_run=False,
        variation_count=1,
        prompt_variant_ids=[],
        prompt_override=None,
        use_library=False,
        prompt_id=None,
        style_anchors=[],
        all_models=False,
        provider=None,
        no_resume=False,
    )
    assert result.status == "success"
    assert result.quality_passed == 1
    assert result.exported == 3


def test_run_pipeline_and_status(tmp_path: Path, monkeypatch):
    runtime = _runtime(tmp_path)
    monkeypatch.setattr(pl.config, "get_config", lambda *_args, **_kwargs: runtime)
    monkeypatch.setattr(pl, "_prepare_prompt_source", lambda **_kwargs: None)
    monkeypatch.setattr(pl, "_ensure_prerequisites", lambda **_kwargs: None)
    monkeypatch.setattr(pl, "_load_pipeline_state", lambda **_kwargs: {"catalog": runtime.catalog_id, "completed_books": {}, "failed_books": {}})
    monkeypatch.setattr(pl, "_save_pipeline_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(pl, "_save_generation_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(pl, "_write_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(pl, "_book_is_complete", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(pl, "_prioritize_books", lambda books, **_kwargs: books)
    monkeypatch.setattr(pl, "_log_progress", lambda **_kwargs: None)

    class _Notifier:
        def __init__(self, **_kwargs):
            pass

        def batch_start(self, **_kwargs):
            return None

        def milestone(self, **_kwargs):
            return None

        def batch_error(self, **_kwargs):
            return None

        def batch_complete(self, **_kwargs):
            return None

    monkeypatch.setattr(pl, "BatchNotifier", _Notifier)
    monkeypatch.setattr(pl, "_run_single_book", lambda **kwargs: _book_result(kwargs["book_number"]))

    result = pl.run_pipeline(
        input_dir=runtime.input_dir,
        output_dir=runtime.output_dir,
        config_overrides={"workers": 1},
        book_numbers=[1, 2],
        resume=True,
        dry_run=False,
        catalog_id=runtime.catalog_id,
    )
    assert result["processed_books"] == 2
    assert result["failed_books"] == 0

    status = pl.get_pipeline_status(runtime.output_dir, catalog_id=runtime.catalog_id)
    assert status["catalog"] == runtime.catalog_id


def test_run_pipeline_empty_and_parallel_skip_paths(tmp_path: Path, monkeypatch):
    runtime = _runtime(tmp_path)
    monkeypatch.setattr(pl.config, "get_config", lambda *_args, **_kwargs: runtime)
    monkeypatch.setattr(pl, "_prepare_prompt_source", lambda **_kwargs: None)
    monkeypatch.setattr(pl, "_ensure_prerequisites", lambda **_kwargs: None)
    monkeypatch.setattr(pl, "_save_pipeline_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(pl, "_save_generation_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(pl, "_write_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(pl, "_log_progress", lambda **_kwargs: None)
    monkeypatch.setattr(pl, "_load_pipeline_state", lambda **_kwargs: {"catalog": runtime.catalog_id, "completed_books": {}, "failed_books": {}})

    class _Notifier:
        def __init__(self, **_kwargs):
            pass

        def batch_start(self, **_kwargs):
            return None

        def milestone(self, **_kwargs):
            return None

        def batch_error(self, **_kwargs):
            return None

        def batch_complete(self, **_kwargs):
            return None

    monkeypatch.setattr(pl, "BatchNotifier", _Notifier)

    # Empty book list early-return path.
    monkeypatch.setattr(pl.config, "get_initial_scope_book_numbers", lambda **_kwargs: [])
    summary_empty = pl.run_pipeline(
        input_dir=runtime.input_dir,
        output_dir=runtime.output_dir,
        config_overrides={"workers": 1},
        book_numbers=None,
        resume=True,
        dry_run=False,
        catalog_id=runtime.catalog_id,
    )
    assert summary_empty["processed_books"] == 0

    # Parallel branch with one skipped and one failure.
    monkeypatch.setattr(pl.config, "get_initial_scope_book_numbers", lambda **_kwargs: [1, 2, 3])
    monkeypatch.setattr(pl, "_book_is_complete", lambda book_number, *_args, **_kwargs: book_number == 1)

    def _run_or_fail(**kwargs):  # type: ignore[no-untyped-def]
        book = kwargs["book_number"]
        if book == 3:
            raise RuntimeError("boom")
        return _book_result(book, "success")

    monkeypatch.setattr(pl, "_run_single_book", _run_or_fail)
    summary_parallel = pl.run_pipeline(
        input_dir=runtime.input_dir,
        output_dir=runtime.output_dir,
        config_overrides={"workers": 2},
        book_numbers=[1, 2, 3],
        resume=True,
        dry_run=False,
        catalog_id=runtime.catalog_id,
    )
    assert summary_parallel["processed_books"] == 3
    assert summary_parallel["skipped_books"] == 1
    assert summary_parallel["failed_books"] == 1


def test_collect_sync_paths_and_sync_output_to_drive(tmp_path: Path, monkeypatch):
    runtime = _runtime(tmp_path)
    # Build output files for passed variants.
    folder = runtime.output_dir / "1. Book One" / "Variant-1"
    _touch(folder / "A.jpg")
    _touch(folder / "A.pdf")
    _touch(folder / "A.ai")
    (runtime.data_dir / "quality_scores.json").write_text(
        json.dumps({"scores": [{"book_number": 1, "variant_id": 1, "passed": True}]}),
        encoding="utf-8",
    )

    selected = pl._collect_passed_sync_paths(
        output_dir=runtime.output_dir,
        books=[1],
        scores_path=runtime.data_dir / "quality_scores.json",
        catalog_path=runtime.book_catalog_path,
    )
    assert len(selected) == 3

    summary = pl._sync_output_to_drive(output_dir=runtime.output_dir, books=[1], runtime=runtime)
    assert summary["failed"] == 0
    assert summary["total_files"] >= 3

    runtime.gdrive_output_folder_id = ""
    with pytest.raises(ValueError):
        pl._sync_output_to_drive(output_dir=runtime.output_dir, books=[1], runtime=runtime)


def test_main_paths(tmp_path: Path, monkeypatch):
    runtime = _runtime(tmp_path)
    monkeypatch.setattr(pl.config, "get_config", lambda *_args, **_kwargs: runtime)
    monkeypatch.setattr(pl, "_install_signal_handlers", lambda: None)

    base_args = {
        "catalog": runtime.catalog_id,
        "input_dir": None,
        "output_dir": None,
        "book": None,
        "books": None,
        "variant": None,
        "variants": None,
        "model": None,
        "models": None,
        "all_models": False,
        "provider": None,
        "prompt_override": None,
        "use_library": False,
        "prompt_id": None,
        "style_anchors": None,
        "intelligent_prompts": False,
        "enrich_first": False,
        "legacy_prompts": False,
        "batch_size": 20,
        "workers": 1,
        "priority": "high,medium,low",
        "resume": True,
        "no_resume": False,
        "dry_run": False,
        "sync": False,
        "estimate": False,
        "notify": False,
        "status": False,
        "test_api_keys": False,
        "retry_failures": False,
    }

    args_status_payload = dict(base_args)
    args_status_payload["status"] = True
    args_status = SimpleNamespace(**args_status_payload)
    monkeypatch.setattr(pl.argparse.ArgumentParser, "parse_args", lambda self: args_status)
    monkeypatch.setattr(pl, "get_pipeline_status", lambda *_args, **_kwargs: {"ok": True})
    assert pl.main() == 0

    args_keys_payload = dict(base_args)
    args_keys_payload["test_api_keys"] = True
    args_keys_payload["provider"] = "openai"
    args_keys = SimpleNamespace(**args_keys_payload)
    monkeypatch.setattr(pl.argparse.ArgumentParser, "parse_args", lambda self: args_keys)
    monkeypatch.setattr(pl, "test_api_keys", lambda **_kwargs: {"providers": []})
    assert pl.main() == 0

    args_retry_payload = dict(base_args)
    args_retry_payload["retry_failures"] = True
    args_retry = SimpleNamespace(**args_retry_payload)
    monkeypatch.setattr(pl.argparse.ArgumentParser, "parse_args", lambda self: args_retry)
    monkeypatch.setattr(
        pl.image_generator,
        "retry_failures",
        lambda **_kwargs: [SimpleNamespace(success=False, book_number=1)],
    )
    assert pl.main() == 1

    args_estimate_payload = dict(base_args)
    args_estimate_payload["estimate"] = True
    args_estimate_payload["books"] = "1-2"
    args_estimate = SimpleNamespace(**args_estimate_payload)
    monkeypatch.setattr(pl.argparse.ArgumentParser, "parse_args", lambda self: args_estimate)
    monkeypatch.setattr(pl, "estimate_batch", lambda **_kwargs: {"estimated_cost": 1.0, "books": 2, "variants_per_model": 1, "estimated_time_hours": 0.1, "workers": 1})
    assert pl.main() == 0

    args_run_payload = dict(base_args)
    args_run_payload["books"] = "1"
    args_run_payload["dry_run"] = False
    args_run = SimpleNamespace(**args_run_payload)
    monkeypatch.setattr(pl.argparse.ArgumentParser, "parse_args", lambda self: args_run)
    monkeypatch.setattr(pl, "run_pipeline", lambda **_kwargs: {"failed_books": 0})
    assert pl.main() == 0


def test_pipeline_helper_edge_paths(tmp_path: Path, monkeypatch):
    runtime = _runtime(tmp_path)

    assert pl._generate_with_model_prompts(
        book_number=1,
        base_prompt="base",
        negative_prompt="neg",
        models=[],
        variants_per_model=1,
        title="Book",
        overrides={},
        explicit_prompt_requested=False,
        output_dir=runtime.tmp_dir,
        resume=True,
        dry_run=True,
        provider_override=None,
    ) == []

    original_resolve = pl._resolve_model_prompt
    monkeypatch.setattr(
        pl,
        "_resolve_model_prompt",
        lambda **kwargs: "A" if kwargs["model"] == "m1" else "B",
    )
    calls = []
    monkeypatch.setattr(
        pl.image_generator,
        "generate_all_models",
        lambda **kwargs: (calls.append(kwargs) or [SimpleNamespace(success=True, cost=0.0)]),
    )
    split = pl._generate_with_model_prompts(
        book_number=1,
        base_prompt="base",
        negative_prompt="neg",
        models=["m1", "m2"],
        variants_per_model=1,
        title="Book",
        overrides={},
        explicit_prompt_requested=False,
        output_dir=runtime.tmp_dir,
        resume=True,
        dry_run=True,
        provider_override=None,
    )
    assert len(split) == 2
    assert len(calls) == 2
    assert calls[0]["models"] == ["m1"]
    assert calls[1]["models"] == ["m2"]
    monkeypatch.setattr(pl, "_resolve_model_prompt", original_resolve)

    missing_overrides = pl._load_model_prompt_overrides(runtime.config_dir / "missing_overrides.json")
    assert missing_overrides == {}
    bad_overrides_path = runtime.config_dir / "bad_overrides.json"
    bad_overrides_path.write_text("{bad-json", encoding="utf-8")
    assert pl._load_model_prompt_overrides(bad_overrides_path) == {}

    assert pl._resolve_model_prompt(
        model="m1",
        title="Book",
        fallback_prompt="fallback",
        overrides={"m1": {"prompt_template": "Ignored"}},
        explicit_prompt_requested=True,
    ) == "fallback"
    assert pl._resolve_model_prompt(
        model="unknown/model",
        title="Book",
        fallback_prompt="fallback",
        overrides={"other": {"prompt_template": "X"}},
        explicit_prompt_requested=False,
    ) == "fallback"

    assert pl._normalize_providers(["", " openai "]) == ["openai"]
    with pytest.raises(KeyError):
        pl._find_book_entry({"books": []}, 99)
    assert pl._find_variant_entry({"variants": [{"variant_id": 7, "prompt": "p"}]}, 3)["variant_id"] == 7
    with pytest.raises(KeyError):
        pl._find_variant_entry({"number": 1, "variants": []}, 1)

    assert pl._parse_books(None) is None
    assert pl._parse_books("1,,2") == [1, 2]
    assert pl._format_api_key_report({"providers": "bad"}) == "No providers checked."
    assert pl._format_api_key_report({"providers": [{"provider": "x", "status": "KEY_VALID", "detail": ""}]}) == "X — KEY_VALID"
    assert pl._resolve_variant_options("invalid", None, runtime) == (runtime.variants_per_cover, [])

    runtime.google_credentials_path = "relative/creds.json"
    creds_path = pl._resolve_credentials_path(runtime)
    assert creds_path == runtime.project_root / "relative/creds.json"


def test_pipeline_probe_and_quality_sync_edge_paths(tmp_path: Path, monkeypatch):
    runtime = _runtime(tmp_path)

    assert pl._probe_provider_key(provider="custom", api_key="k", timeout=1.0)[0] is False

    def _raise_request(_url, headers=None, timeout=None):  # type: ignore[no-untyped-def]
        raise pl.requests.RequestException("network down")

    monkeypatch.setattr(pl.requests, "get", _raise_request)
    assert pl._probe_provider_key(provider="openrouter", api_key="k", timeout=1.0)[0] is False

    def _fake_status(url, headers=None, timeout=None):  # type: ignore[no-untyped-def]
        if "openrouter" in url or "replicate" in url or "fal.ai" in url:
            return SimpleNamespace(status_code=200, text="ok")
        return SimpleNamespace(status_code=429, text=("x" * 300))

    monkeypatch.setattr(pl.requests, "get", _fake_status)
    assert pl._probe_provider_key(provider="openrouter", api_key="k", timeout=1.0)[0] is True
    assert pl._probe_provider_key(provider="replicate", api_key="k", timeout=1.0)[0] is True
    assert pl._probe_provider_key(provider="fal", api_key="k", timeout=1.0)[0] is True
    detail = pl._probe_provider_key(provider="google", api_key="k", timeout=1.0)[1]
    assert detail.startswith("HTTP 429:")
    assert detail.endswith("...")

    quality_path = runtime.data_dir / "quality_scores.json"
    quality_path.write_text("{bad-json", encoding="utf-8")
    assert pl._books_below_quality_threshold(runtime=runtime, threshold=0.5) == set()
    quality_path.write_text(json.dumps({"scores": {"bad": 1}}), encoding="utf-8")
    assert pl._books_below_quality_threshold(runtime=runtime, threshold=0.5) == set()
    quality_path.write_text(
        json.dumps(
            {
                    "scores": [
                        "not-dict",
                        {"book_number": "bad", "overall_score": 0.1},
                        {"book_number": -1, "overall_score": 0.1},
                        {"book_number": 9, "overall_score": "not-a-number"},
                    ]
                }
            ),
            encoding="utf-8",
        )
    assert 9 not in pl._books_below_quality_threshold(runtime=runtime, threshold=0.5)

    monkeypatch.setattr(pl, "_books_below_quality_threshold", lambda **_kwargs: {1})
    monkeypatch.setattr(pl, "_book_is_complete", lambda book, *_args, **_kwargs: book == 3)
    ordered = pl._prioritize_books(
        [1, 1, 2, 3],
        output_dir=runtime.output_dir,
        runtime=runtime,
        priority_order="high,unknown",
        state={"failed_books": {}, "completed_books": {}},
    )
    assert ordered[0] == 1
    assert 2 in ordered and 3 in ordered

    history = runtime.data_dir / "generation_history.json"
    history.write_text("{bad-json", encoding="utf-8")
    estimate = pl.estimate_batch(runtime=runtime, books=[1], models=["m"], variants_per_model=1, workers=1)
    assert estimate["estimated_time_hours"] > 0

    missing_scores = pl._collect_passed_sync_paths(
        output_dir=runtime.output_dir,
        books=[1],
        scores_path=runtime.data_dir / "missing.json",
        catalog_path=runtime.book_catalog_path,
    )
    assert missing_scores == []

    bad_scores = runtime.data_dir / "bad_scores.json"
    bad_scores.write_text("{bad-json", encoding="utf-8")
    assert pl._collect_passed_sync_paths(
        output_dir=runtime.output_dir,
        books=[1],
        scores_path=bad_scores,
        catalog_path=runtime.book_catalog_path,
    ) == []

    complex_scores = runtime.data_dir / "complex_scores.json"
    complex_scores.write_text(
        json.dumps(
            {
                "scores": [
                    "not-dict",
                    {"book_number": 1, "variant_id": 1, "passed": False},
                    {"book_number": "bad", "variant_id": 1, "passed": True},
                    {"book_number": 1, "variant_id": "bad", "passed": True},
                    {"book_number": 1, "variant_id": 2, "passed": True},
                ]
            }
        ),
        encoding="utf-8",
    )
    runtime.book_catalog_path.write_text(
        json.dumps(
            [
                {"number": "bad", "folder_name": "ignore"},
                {"number": 1, "folder_name": "1. Book One copy"},
            ]
        ),
        encoding="utf-8",
    )
    variant_dir = runtime.output_dir / "1. Book One" / "Variant-2"
    variant_dir.mkdir(parents=True, exist_ok=True)
    (variant_dir / "sub").mkdir(parents=True, exist_ok=True)
    _touch(variant_dir / "keep.jpg")
    _touch(variant_dir / "ignore.txt")
    picked = pl._collect_passed_sync_paths(
        output_dir=runtime.output_dir,
        books=[1, 2],
        scores_path=complex_scores,
        catalog_path=runtime.book_catalog_path,
    )
    assert picked == ["1. Book One/Variant-2/keep.jpg"]

    runtime.gdrive_output_folder_id = "gdrive-folder-id"
    runtime.google_credentials_path = ""
    with pytest.raises(FileNotFoundError):
        pl._sync_output_to_drive(output_dir=runtime.output_dir, books=[1], runtime=runtime)

    runtime.gdrive_output_folder_id = "local:/tmp/mirror"
    monkeypatch.setattr(pl, "_collect_passed_sync_paths", lambda **_kwargs: [])
    empty_sync = pl._sync_output_to_drive(output_dir=runtime.output_dir, books=[1], runtime=runtime)
    assert empty_sync["total_files"] == 0


def test_run_pipeline_shutdown_and_main_sync_path(tmp_path: Path, monkeypatch):
    runtime = _runtime(tmp_path)
    monkeypatch.setattr(pl.config, "get_config", lambda *_args, **_kwargs: runtime)
    monkeypatch.setattr(pl, "_prepare_prompt_source", lambda **_kwargs: None)
    monkeypatch.setattr(pl, "_ensure_prerequisites", lambda **_kwargs: None)
    monkeypatch.setattr(pl, "_save_pipeline_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(pl, "_save_generation_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(pl, "_write_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(pl, "_book_is_complete", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(pl, "_prioritize_books", lambda books, **_kwargs: books)
    monkeypatch.setattr(pl, "_log_progress", lambda **_kwargs: None)
    monkeypatch.setattr(pl, "_load_pipeline_state", lambda **_kwargs: {"catalog": runtime.catalog_id, "completed_books": {}, "failed_books": {}})

    class _Notifier:
        def __init__(self, **_kwargs):
            self.milestones = 0

        def batch_start(self, **_kwargs):
            return None

        def milestone(self, **_kwargs):
            self.milestones += 1
            return None

        def batch_error(self, **_kwargs):
            return None

        def batch_complete(self, **_kwargs):
            return None

    monkeypatch.setattr(pl, "BatchNotifier", _Notifier)

    def _run_and_request_shutdown(**kwargs):  # type: ignore[no-untyped-def]
        if kwargs["book_number"] == 1:
            pl._SHUTDOWN_REQUESTED = True
        return _book_result(kwargs["book_number"])

    monkeypatch.setattr(pl, "_run_single_book", _run_and_request_shutdown)
    summary = pl.run_pipeline(
        input_dir=runtime.input_dir,
        output_dir=runtime.output_dir,
        config_overrides={"workers": 1},
        book_numbers=[1, 2],
        resume=True,
        dry_run=False,
        catalog_id=runtime.catalog_id,
    )
    assert summary["interrupted"] is True
    assert summary["processed_books"] == 2

    args = SimpleNamespace(
        catalog=runtime.catalog_id,
        input_dir=None,
        output_dir=None,
        book=1,
        books=None,
        variant=None,
        variants=None,
        model=None,
        models=None,
        all_models=False,
        provider=None,
        prompt_override=None,
        use_library=False,
        prompt_id=None,
        style_anchors=None,
        intelligent_prompts=False,
        enrich_first=False,
        legacy_prompts=False,
        batch_size=20,
        workers=1,
        priority="high,medium,low",
        resume=True,
        no_resume=False,
        dry_run=False,
        sync=True,
        estimate=False,
        notify=False,
        status=False,
        test_api_keys=False,
        retry_failures=False,
    )
    monkeypatch.setattr(pl.argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(pl, "_install_signal_handlers", lambda: None)
    monkeypatch.setattr(pl, "run_pipeline", lambda **_kwargs: {"failed_books": 0})
    monkeypatch.setattr(pl, "_sync_output_to_drive", lambda **_kwargs: {"uploaded": 1})
    assert pl.main() == 0
