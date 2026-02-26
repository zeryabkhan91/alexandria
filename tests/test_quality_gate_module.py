from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from PIL import Image

from src import quality_gate as qg


def _save_image(path: Path, *, mode: str = "RGB", size: tuple[int, int] = (1024, 1024), color=(120, 90, 70)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new(mode, size, color=color).save(path, format="PNG")


def test_core_quality_components():
    blank = np.zeros((1024, 1024, 3), dtype=np.uint8)
    score, issues = qg._technical_quality(blank)
    assert 0.0 <= score <= 1.0
    assert "blank_or_solid" in issues

    warm = np.full((256, 256, 3), [170, 130, 90], dtype=np.uint8)
    c_score, c_issues = qg._color_compatibility(warm)
    assert 0.0 <= c_score <= 1.0
    assert isinstance(c_issues, list)

    noisy = np.random.randint(0, 255, size=(256, 256, 3), dtype=np.uint8)
    a_score, a_issues = qg._artifact_score(noisy)
    assert 0.0 <= a_score <= 1.0
    assert isinstance(a_issues, list)

    palette_score, palette_issues = qg._palette_alignment(warm)
    assert 0.0 <= palette_score <= 1.0
    assert isinstance(palette_issues, list)

    blur_score, blur_issues = qg._blur_score(blank)
    assert 0.0 <= blur_score <= 1.0
    assert isinstance(blur_issues, list)

    text_score, text_issues = qg._text_contamination_score(noisy)
    assert 0.0 <= text_score <= 1.0
    assert isinstance(text_issues, list)

    alpha = np.zeros((128, 128), dtype=np.uint8)
    alpha[10:118, 10:118] = 255
    border_score, border_issues = qg._border_safety_score(alpha)
    assert 0.0 <= border_score <= 1.0
    assert isinstance(border_issues, list)

    prompt_score, prompt_issues = qg._prompt_relevance_score(prompt="whale at sea", rgb=warm)
    assert 0.0 <= prompt_score <= 1.0
    assert isinstance(prompt_issues, list)


def test_helpers_and_hashing():
    assert qg._parse_variant_id("variant_3") == 3
    assert qg._parse_variant_id("x") == 0
    assert qg._clip(-1.0) == 0.0
    assert qg._clip(2.0) == 1.0

    img = Image.new("RGB", (64, 64), color=(100, 100, 100))
    h1 = qg._average_hash(img)
    h2 = qg._average_hash(img)
    assert qg._hamming_distance(h1, h2) == 0
    assert qg._normalize_model_key("openai__gpt-image-1") == "openai/gpt-image-1"


def test_collect_generated_images_patterns(tmp_path: Path):
    _save_image(tmp_path / "1" / "model_a" / "variant_1.png")
    _save_image(tmp_path / "2" / "variant_2.png")
    rows = qg._collect_generated_images(tmp_path)
    assert len(rows) == 2
    assert {row["book_number"] for row in rows} == {1, 2}


def test_get_prompt_context_fallbacks():
    payload = {
        "books": [
            {
                "number": 1,
                "variants": [
                    {"variant_id": 1, "prompt": "p1", "negative_prompt": "n1"},
                ],
            }
        ]
    }
    exact = qg._get_prompt_context(payload, 1, 1)
    assert exact["prompt"] == "p1"
    fallback = qg._get_prompt_context(payload, 1, 9)
    assert fallback["prompt"] == "p1"
    missing = qg._get_prompt_context(payload, 2, 1)
    assert missing == {"prompt": "", "negative_prompt": ""}


def test_tweak_prompt_and_retry_log_writers(tmp_path: Path):
    p, n = qg._tweak_prompt("base", "neg", 2)
    assert "base" in p and "neg" in n

    retry_path = tmp_path / "retry.json"
    rank_path = tmp_path / "rank.json"
    qg._write_retry_log([{"book": 1}], retry_path)
    qg._write_model_rankings([{"model": "x"}], rank_path)
    assert json.loads(retry_path.read_text(encoding="utf-8"))["retry_count"] == 1
    assert json.loads(rank_path.read_text(encoding="utf-8"))["rankings"][0]["model"] == "x"


def test_quality_gate_json_writers_use_atomic_helper(tmp_path: Path, monkeypatch):
    calls: list[tuple[Path, object]] = []

    def _capture(path, payload):  # type: ignore[no-untyped-def]
        calls.append((path, payload))

    monkeypatch.setattr(qg.safe_json, "atomic_write_json", _capture)

    score_path = tmp_path / "quality_scores.json"
    retry_path = tmp_path / "retry.json"
    rank_path = tmp_path / "rankings.json"

    image_path = tmp_path / "img.png"
    _save_image(image_path)
    score = qg.score_image(image_path, threshold=0.5, book_number=1, variant_id=1, model="model_a")
    qg._write_quality_scores([score], score_path)
    qg._write_retry_log([{"book_number": 1}], retry_path)
    qg._write_model_rankings([{"model": "model_a"}], rank_path)

    assert [path for path, _payload in calls] == [score_path, retry_path, rank_path]


def test_score_image_and_batch(tmp_path: Path, monkeypatch):
    img_path = tmp_path / "1" / "model_a" / "variant_1.png"
    _save_image(img_path)

    score = qg.score_image(img_path, threshold=0.2, book_number=1, variant_id=1, model="m")
    assert score.book_number == 1
    assert 0.0 <= score.overall_score <= 1.0
    assert 0.0 <= score.blur_score <= 1.0
    assert 0.0 <= score.text_contamination_score <= 1.0

    monkeypatch.setattr(qg, "_distinctiveness_score", lambda **_kwargs: 1.0)
    monkeypatch.setattr(qg, "_build_prompt_lookup", lambda: {(1, 1, "model_a"): "whale at sea"})
    scores = qg.score_batch(tmp_path, threshold=0.2)
    assert len(scores) == 1
    assert scores[0].model == "model_a"
    assert scores[0].prompt_relevance_score >= 0.0


def test_diversity_application(tmp_path: Path):
    p1 = tmp_path / "a.png"
    p2 = tmp_path / "b.png"
    _save_image(p1, color=(130, 130, 130))
    _save_image(p2, color=(130, 130, 130))
    s1 = qg.score_image(p1, threshold=0.99, book_number=1, variant_id=1, model="m")
    s2 = qg.score_image(p2, threshold=0.99, book_number=1, variant_id=2, model="m")
    qg._apply_diversity_scores([s1, s2], threshold=0.99)
    assert s1.diversity_score <= 1.0
    assert isinstance(s1.passed, bool)


def test_write_quality_scores_and_report(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(qg.config, "get_config", lambda: SimpleNamespace(resolve_model_provider=lambda m: "openrouter"))
    p1 = tmp_path / "v1.png"
    p2 = tmp_path / "v2.png"
    _save_image(p1)
    _save_image(p2, color=(90, 110, 140))
    s1 = qg.score_image(p1, threshold=0.5, book_number=1, variant_id=1, model="m1")
    s2 = qg.score_image(p2, threshold=0.5, book_number=1, variant_id=2, model="m2")
    scores = [s1, s2]

    out_scores = tmp_path / "quality_scores.json"
    out_report = tmp_path / "quality_report.md"
    out_rank = tmp_path / "rankings.json"
    out_retry = tmp_path / "retry.json"

    qg._write_quality_scores(scores, out_scores)
    payload = json.loads(out_scores.read_text(encoding="utf-8"))
    assert payload["summary"]["total"] == 2

    rankings = qg.build_model_rankings(scores)
    assert len(rankings) == 2
    qg.generate_quality_report(scores, out_report, model_rankings=rankings)
    assert "Quality Report" in out_report.read_text(encoding="utf-8")

    monkeypatch.setattr(qg, "score_batch", lambda *_a, **_k: scores)
    monkeypatch.setattr(qg, "_retry_failed_images", lambda **kwargs: (kwargs["scores"], []))
    out = qg.run_quality_gate(
        generated_dir=tmp_path,
        prompts_path=tmp_path / "prompts.json",
        perform_retries=True,
        output_scores_path=out_scores,
        output_report_path=out_report,
        retry_log_path=out_retry,
        model_rankings_path=out_rank,
    )
    assert len(out) == 2


def test_prompt_lookup_and_resolution(tmp_path: Path, monkeypatch):
    data_dir = tmp_path / "data"
    config_dir = tmp_path / "config"
    data_dir.mkdir(parents=True, exist_ok=True)
    config_dir.mkdir(parents=True, exist_ok=True)

    history_path = data_dir / "generation_history.json"
    prompts_path = config_dir / "book_prompts.json"
    history_path.write_text(
        json.dumps(
            {
                "items": [
                    {"book_number": 1, "variant": 2, "model": "openai/gpt-image-1", "prompt": "history prompt"},
                ]
            }
        ),
        encoding="utf-8",
    )
    prompts_path.write_text(
        json.dumps({"books": [{"number": 1, "variants": [{"variant_id": 2, "prompt": "prompt file value"}]}]}),
        encoding="utf-8",
    )

    runtime = SimpleNamespace(data_dir=data_dir, prompts_path=prompts_path)
    monkeypatch.setattr(qg.config, "get_config", lambda: runtime)
    lookup = qg._build_prompt_lookup()
    assert lookup[(1, 2, "openai/gpt-image-1")] == "history prompt"
    assert qg._resolve_candidate_prompt(prompt_lookup=lookup, book_number=1, variant_id=2, model="openai__gpt-image-1")


def test_prompt_relevance_branches():
    marine = np.full((128, 128, 3), [70, 120, 210], dtype=np.uint8)
    gothic = np.full((128, 128, 3), [20, 20, 30], dtype=np.uint8)
    warm = np.full((128, 128, 3), [180, 120, 60], dtype=np.uint8)

    m_score, _ = qg._prompt_relevance_score(prompt="stormy sea with whale ship adventure", rgb=marine)
    g_score, _ = qg._prompt_relevance_score(prompt="gothic night castle scene", rgb=gothic)
    w_score, _ = qg._prompt_relevance_score(prompt="golden fire torch portrait", rgb=warm)
    assert 0.0 <= m_score <= 1.0
    assert 0.0 <= g_score <= 1.0
    assert 0.0 <= w_score <= 1.0


def test_distinctiveness_score_threshold_bands(monkeypatch, tmp_path: Path):
    runtime = SimpleNamespace(
        output_dir=tmp_path / "Output Covers",
        book_catalog_path=tmp_path / "catalog.json",
        data_dir=tmp_path / "data",
        config_dir=tmp_path / "config",
    )
    runtime.output_dir.mkdir(parents=True, exist_ok=True)
    runtime.data_dir.mkdir(parents=True, exist_ok=True)
    runtime.config_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(qg.config, "get_config", lambda: runtime)

    image_path = tmp_path / "img.png"
    _save_image(image_path)
    monkeypatch.setattr(
        qg.similarity_detector,
        "check_generated_image_against_winners",
        lambda **_kwargs: {"similarity": 0.5},
    )
    assert qg._distinctiveness_score(image_path=image_path, book_number=1) == 1.0
    monkeypatch.setattr(
        qg.similarity_detector,
        "check_generated_image_against_winners",
        lambda **_kwargs: {"similarity": 0.2},
    )
    assert 0.0 <= qg._distinctiveness_score(image_path=image_path, book_number=1) <= 1.0


def test_retry_failed_images_success_and_exhaustion(tmp_path: Path, monkeypatch):
    img_path = tmp_path / "1" / "model_a" / "variant_1.png"
    _save_image(img_path)
    prompt_path = tmp_path / "prompts.json"
    prompt_path.write_text(
        json.dumps(
            {
                "books": [
                    {
                        "number": 1,
                        "variants": [
                            {"variant_id": 1, "prompt": "base prompt", "negative_prompt": "neg"},
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    runtime = SimpleNamespace(
        image_width=1024,
        image_height=1024,
        resolve_model_provider=lambda _m: "openrouter",
        output_dir=tmp_path / "Output Covers",
        book_catalog_path=tmp_path / "catalog.json",
        data_dir=tmp_path / "data",
        config_dir=tmp_path / "config",
    )
    runtime.output_dir.mkdir(parents=True, exist_ok=True)
    runtime.data_dir.mkdir(parents=True, exist_ok=True)
    runtime.config_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(qg.config, "get_config", lambda: runtime)
    monkeypatch.setattr(qg, "_distinctiveness_score", lambda **_kwargs: 1.0)
    monkeypatch.setattr(
        qg.image_generator,
        "generate_image",
        lambda **_kwargs: Image.new("RGBA", (1024, 1024), (120, 100, 80, 255)).tobytes(),
    )

    # generate_image must return PNG bytes in this code path.
    def _png_bytes(**_kwargs):  # type: ignore[no-untyped-def]
        out = Image.new("RGBA", (1024, 1024), (120, 100, 80, 255))
        import io
        buf = io.BytesIO()
        out.save(buf, format="PNG")
        return buf.getvalue()

    monkeypatch.setattr(qg.image_generator, "generate_image", _png_bytes)

    failing = qg.score_image(img_path, threshold=0.99, book_number=1, variant_id=1, model="model_a")
    scores, retry_log = qg._retry_failed_images(
        scores=[failing],
        prompts_path=prompt_path,
        threshold=0.2,
        max_retries=1,
    )
    assert len(scores) == 1
    assert retry_log

    monkeypatch.setattr(
        qg.image_generator,
        "generate_image",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    failing2 = qg.score_image(img_path, threshold=0.99, book_number=1, variant_id=1, model="model_a")
    scores2, _ = qg._retry_failed_images(
        scores=[failing2],
        prompts_path=prompt_path,
        threshold=0.99,
        max_retries=1,
    )
    assert scores2[0].recommendation in {"manual_review", "regenerate", "accept"}


def test_quality_gate_main(monkeypatch, tmp_path: Path):
    args = argparse.Namespace(
        generated_dir=tmp_path,
        prompts_path=tmp_path / "prompts.json",
        threshold=0.7,
        max_retries=1,
        no_retries=True,
    )
    monkeypatch.setattr(qg.argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(qg, "run_quality_gate", lambda **_kwargs: [])
    assert qg.main() == 0
