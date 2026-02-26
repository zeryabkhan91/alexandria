"""Prompt 2B quality gate: score, filter, retry, and rank generated images."""

from __future__ import annotations

import argparse
import logging
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

try:
    from src import config
    from src import image_generator
    from src import safe_json
    from src import similarity_detector
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import image_generator  # type: ignore
    import safe_json  # type: ignore
    import similarity_detector  # type: ignore
    from logger import get_logger  # type: ignore

logger = get_logger(__name__)


@dataclass(slots=True)
class QualityScore:
    """Quality assessment for a single generated image."""

    book_number: int
    variant_id: int
    model: str
    image_path: Path
    technical_score: float
    color_score: float
    palette_score: float
    artifact_score: float
    blur_score: float
    text_contamination_score: float
    border_safety_score: float
    prompt_relevance_score: float
    distinctiveness_score: float
    diversity_score: float
    overall_score: float
    passed: bool
    issues: list[str]
    recommendation: str
    retries: int = 0

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["image_path"] = str(self.image_path)
        return payload


def score_image(
    image_path: Path,
    *,
    threshold: float = 0.7,
    book_number: int = 0,
    variant_id: int = 0,
    model: str = "unknown",
    distinctiveness_score: float = 1.0,
    prompt: str = "",
) -> QualityScore:
    """Score a single generated image."""
    image_rgba = Image.open(image_path).convert("RGBA")
    image = image_rgba.convert("RGB")
    rgb = np.array(image, dtype=np.uint8)
    alpha = np.array(image_rgba, dtype=np.uint8)[..., 3]

    technical_score, technical_issues = _technical_quality(rgb)
    color_score, color_issues = _color_compatibility(rgb)
    palette_score, palette_issues = _palette_alignment(rgb)
    artifact_score, artifact_issues = _artifact_score(rgb)
    blur_score, blur_issues = _blur_score(rgb)
    text_score, text_issues = _text_contamination_score(rgb)
    border_score, border_issues = _border_safety_score(alpha)
    prompt_score, prompt_issues = _prompt_relevance_score(prompt=prompt, rgb=rgb)

    issues = (
        technical_issues
        + color_issues
        + palette_issues
        + artifact_issues
        + blur_issues
        + text_issues
        + border_issues
        + prompt_issues
    )
    # Weighted for the Alexandria style targets:
    # emphasize technical clarity + artifact cleanliness while adding explicit
    # checks for text contamination, blur, palette fit, edge safety, and
    # coarse prompt-to-image alignment.
    overall = (
        (0.24 * technical_score)
        + (0.08 * color_score)
        + (0.10 * palette_score)
        + (0.20 * artifact_score)
        + (0.07 * blur_score)
        + (0.06 * text_score)
        + (0.05 * border_score)
        + (0.04 * prompt_score)
        + (0.16 * _clip(distinctiveness_score))
    )

    passed = overall >= threshold
    recommendation = "accept" if passed else "regenerate"

    return QualityScore(
        book_number=book_number,
        variant_id=variant_id,
        model=model,
        image_path=image_path,
        technical_score=round(technical_score, 4),
        color_score=round(color_score, 4),
        palette_score=round(palette_score, 4),
        artifact_score=round(artifact_score, 4),
        blur_score=round(blur_score, 4),
        text_contamination_score=round(text_score, 4),
        border_safety_score=round(border_score, 4),
        prompt_relevance_score=round(prompt_score, 4),
        distinctiveness_score=round(_clip(distinctiveness_score), 4),
        diversity_score=1.0,
        overall_score=round(overall, 4),
        passed=passed,
        issues=issues,
        recommendation=recommendation,
    )


def score_batch(
    image_dir: Path,
    *,
    threshold: float = 0.7,
) -> list[QualityScore]:
    """Score all images in a generated-image directory."""
    candidates = _collect_generated_images(image_dir)
    prompt_lookup = _build_prompt_lookup()
    scores: list[QualityScore] = []

    for item in candidates:
        distinctiveness = _distinctiveness_score(
            image_path=item["image_path"],
            book_number=item["book_number"],
        )
        prompt = _resolve_candidate_prompt(
            prompt_lookup=prompt_lookup,
            book_number=item["book_number"],
            variant_id=item["variant_id"],
            model=item["model"],
        )
        score = score_image(
            item["image_path"],
            threshold=threshold,
            book_number=item["book_number"],
            variant_id=item["variant_id"],
            model=item["model"],
            distinctiveness_score=distinctiveness,
            prompt=prompt,
        )
        scores.append(score)

    _apply_diversity_scores(scores, threshold=threshold)
    return scores


def run_quality_gate(
    generated_dir: Path,
    *,
    prompts_path: Path = config.PROMPTS_PATH,
    threshold: float = 0.7,
    max_retries: int = 3,
    perform_retries: bool = True,
    output_scores_path: Path | None = None,
    output_report_path: Path | None = None,
    retry_log_path: Path | None = None,
    model_rankings_path: Path | None = None,
) -> list[QualityScore]:
    """Evaluate, retry failed generations, and write all quality outputs."""
    runtime = config.get_config()
    catalog_id = getattr(runtime, "catalog_id", None)
    data_dir = getattr(runtime, "data_dir", None)
    output_scores_path = output_scores_path or config.quality_scores_path(catalog_id=catalog_id, data_dir=data_dir)
    output_report_path = output_report_path or config.catalog_scoped_data_path(
        "quality_report.md",
        catalog_id=catalog_id,
        data_dir=data_dir,
    )
    retry_log_path = retry_log_path or config.catalog_scoped_data_path(
        "retry_log.json",
        catalog_id=catalog_id,
        data_dir=data_dir,
    )
    model_rankings_path = model_rankings_path or config.catalog_scoped_data_path(
        "model_rankings.json",
        catalog_id=catalog_id,
        data_dir=data_dir,
    )

    scores = score_batch(generated_dir, threshold=threshold)

    retry_log: list[dict[str, Any]] = []
    if perform_retries:
        scores, retry_log = _retry_failed_images(
            scores=scores,
            prompts_path=prompts_path,
            threshold=threshold,
            max_retries=max_retries,
        )

    model_rankings = build_model_rankings(scores)

    _write_quality_scores(scores, output_scores_path)
    _write_retry_log(retry_log, retry_log_path)
    _write_model_rankings(model_rankings, model_rankings_path)
    generate_quality_report(scores, output_report_path, model_rankings=model_rankings)

    return scores


def build_model_rankings(scores: list[QualityScore]) -> list[dict[str, Any]]:
    """Aggregate model-level quality stats for leaderboard ranking."""
    grouped: dict[str, list[QualityScore]] = {}
    for score in scores:
        grouped.setdefault(score.model, []).append(score)

    ranking: list[dict[str, Any]] = []
    runtime = config.get_config()

    for model, items in grouped.items():
        count = len(items)
        passed = sum(1 for item in items if item.passed)
        avg_score = statistics.mean(item.overall_score for item in items) if items else 0.0
        avg_technical = statistics.mean(item.technical_score for item in items) if items else 0.0
        avg_color = statistics.mean(item.color_score for item in items) if items else 0.0
        avg_palette = statistics.mean(item.palette_score for item in items) if items else 0.0
        avg_artifact = statistics.mean(item.artifact_score for item in items) if items else 0.0
        avg_blur = statistics.mean(item.blur_score for item in items) if items else 0.0
        avg_text = statistics.mean(item.text_contamination_score for item in items) if items else 0.0
        avg_border = statistics.mean(item.border_safety_score for item in items) if items else 0.0
        avg_prompt = statistics.mean(item.prompt_relevance_score for item in items) if items else 0.0
        avg_distinctiveness = statistics.mean(item.distinctiveness_score for item in items) if items else 0.0

        ranking.append(
            {
                "model": model,
                "provider": runtime.resolve_model_provider(model),
                "images_scored": count,
                "passes": passed,
                "pass_rate": round((passed / count) if count else 0.0, 4),
                "average_score": round(avg_score, 4),
                "average_technical": round(avg_technical, 4),
                "average_color": round(avg_color, 4),
                "average_palette": round(avg_palette, 4),
                "average_artifact": round(avg_artifact, 4),
                "average_blur": round(avg_blur, 4),
                "average_text_contamination": round(avg_text, 4),
                "average_border_safety": round(avg_border, 4),
                "average_prompt_relevance": round(avg_prompt, 4),
                "average_distinctiveness": round(avg_distinctiveness, 4),
            }
        )

    ranking.sort(key=lambda item: (item["average_score"], item["pass_rate"]), reverse=True)
    return ranking


def generate_quality_report(
    scores: list[QualityScore],
    output_path: Path,
    *,
    model_rankings: list[dict[str, Any]] | None = None,
) -> None:
    """Generate a human-readable quality report markdown."""
    model_rankings = model_rankings or build_model_rankings(scores)

    total = len(scores)
    passed = sum(1 for item in scores if item.passed)
    failed = total - passed
    flagged = sum(1 for item in scores if item.recommendation == "manual_review")

    by_book: dict[int, list[QualityScore]] = {}
    for score in scores:
        by_book.setdefault(score.book_number, []).append(score)

    lines: list[str] = [
        "# Quality Report",
        "",
        f"- Total images scored: **{total}**",
        f"- Passed: **{passed}**",
        f"- Failed: **{failed}**",
        f"- Flagged manual review: **{flagged}**",
        "",
        "## Model Leaderboard",
        "",
        "| Rank | Model | Provider | Avg Score | Pass Rate | Images |",
        "|---|---|---|---:|---:|---:|",
    ]

    for idx, item in enumerate(model_rankings, start=1):
        lines.append(
            f"| {idx} | `{item['model']}` | `{item['provider']}` | {item['average_score']:.3f} | {item['pass_rate']:.2%} | {item['images_scored']} |"
        )

    lines.extend(["", "## Per-Book Summary", ""])
    for book_number in sorted(by_book):
        entries = by_book[book_number]
        book_avg = statistics.mean(entry.overall_score for entry in entries)
        book_pass = sum(1 for entry in entries if entry.passed)
        lines.append(
            f"- Book {book_number}: {book_pass}/{len(entries)} pass, avg score {book_avg:.3f}"
        )

    lines.extend(["", "## Lowest Scoring Images", ""])
    for item in sorted(scores, key=lambda row: row.overall_score)[:10]:
        lines.append(
            f"- Book {item.book_number} | Variant {item.variant_id} | Model `{item.model}` | score {item.overall_score:.3f} | {item.recommendation}"
        )
        if item.issues:
            lines.append(f"  - Issues: {', '.join(item.issues)}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _technical_quality(rgb: np.ndarray) -> tuple[float, list[str]]:
    issues: list[str] = []
    h, w = rgb.shape[:2]

    resolution_score = min(w / 1024.0, h / 1024.0)
    if (w, h) != (1024, 1024):
        issues.append(f"non-1024 resolution ({w}x{h})")

    std = float(rgb.std())
    dynamic_range = float(int(rgb.max()) - int(rgb.min()))

    gray = rgb.mean(axis=2)
    grad_x = np.abs(np.diff(gray, axis=1)).mean() if w > 1 else 0.0
    grad_y = np.abs(np.diff(gray, axis=0)).mean() if h > 1 else 0.0
    sharpness = float((grad_x + grad_y) / 2.0)

    blank_like = std < 6.0 or dynamic_range < 12.0
    extreme_noise = std > 95.0

    if blank_like:
        issues.append("blank_or_solid")
    if extreme_noise:
        issues.append("extreme_noise")

    std_score = _clip((std - 8.0) / 55.0)
    range_score = _clip((dynamic_range - 20.0) / 180.0)
    sharp_score = _clip((sharpness - 2.0) / 12.0)

    score = (0.35 * _clip(resolution_score)) + (0.25 * std_score) + (0.20 * range_score) + (0.20 * sharp_score)
    if blank_like:
        score *= 0.15
    if extreme_noise:
        score *= 0.65

    return _clip(score), issues


def _color_compatibility(rgb: np.ndarray) -> tuple[float, list[str]]:
    issues: list[str] = []

    arr = rgb.astype(np.float32)
    r = arr[..., 0]
    g = arr[..., 1]
    b = arr[..., 2]

    warm_mask = (r > g) & (g >= b)
    warm_ratio = float(warm_mask.mean())

    mean_r = float(r.mean())
    mean_g = float(g.mean())
    mean_b = float(b.mean())

    warmth_index = (mean_r + 0.35 * mean_g) - mean_b
    warm_balance_score = _clip((warmth_index + 35.0) / 140.0)

    # Broad target window to support both warm paintings and sepia engravings.
    ratio_score = 1.0 - abs(warm_ratio - 0.42) / 0.72
    ratio_score = _clip(ratio_score)

    channel_spread = np.maximum.reduce([r, g, b]) - np.minimum.reduce([r, g, b])
    mean_chroma = float(channel_spread.mean())
    chroma_score = _clip((mean_chroma - 8.0) / 60.0)
    sepia_alignment = _clip(((mean_r - mean_b) - 6.0) / 60.0)
    style_flex_score = max(chroma_score, sepia_alignment)

    if warmth_index < -10.0:
        issues.append("too_cold_for_cover_palette")
    if warm_ratio > 0.96 and mean_chroma < 6.0:
        issues.append("overly_monochrome_warm")

    score = (0.45 * warm_balance_score) + (0.35 * ratio_score) + (0.20 * style_flex_score)
    return _clip(score), issues


def _palette_alignment(rgb: np.ndarray) -> tuple[float, list[str]]:
    issues: list[str] = []
    arr = rgb.astype(np.float32)
    flat = arr.reshape(-1, 3)
    if flat.size == 0:
        return 0.0, ["empty_image"]

    # Navy/gold palette anchors based on the cover system.
    palette = np.array(
        [
            [26.0, 39.0, 68.0],   # navy
            [188.0, 150.0, 90.0], # warm gold
            [122.0, 94.0, 62.0],  # bronze
        ],
        dtype=np.float32,
    )
    distances = np.linalg.norm(flat[:, None, :] - palette[None, :, :], axis=2)
    nearest = distances.min(axis=1)
    coverage = float((nearest < 78.0).mean())

    warm_ratio = float(((flat[:, 0] > flat[:, 2]) & (flat[:, 1] >= flat[:, 2] * 0.7)).mean())
    cool_ratio = float(((flat[:, 2] > flat[:, 0]) & (flat[:, 2] > flat[:, 1])).mean())

    coverage_score = _clip((coverage - 0.08) / 0.55)
    warm_score = _clip((warm_ratio - 0.12) / 0.52)
    cool_score = _clip((cool_ratio - 0.10) / 0.60)
    score = (0.60 * coverage_score) + (0.20 * warm_score) + (0.20 * cool_score)

    if coverage < 0.06:
        issues.append("palette_mismatch")
    if warm_ratio < 0.05 and cool_ratio < 0.08:
        issues.append("palette_low_harmony")

    return _clip(score), issues


def _blur_score(rgb: np.ndarray) -> tuple[float, list[str]]:
    issues: list[str] = []
    gray = rgb.astype(np.float32).mean(axis=2)
    if gray.size == 0:
        return 0.0, ["empty_image"]

    dx = np.diff(gray, axis=1)
    dy = np.diff(gray, axis=0)
    grad_energy = float(np.var(dx) + np.var(dy))
    lap = np.abs(np.diff(gray, n=2, axis=0)).mean() + np.abs(np.diff(gray, n=2, axis=1)).mean()

    grad_score = _clip((grad_energy - 18.0) / 220.0)
    lap_score = _clip((float(lap) - 1.8) / 10.0)
    score = (0.65 * grad_score) + (0.35 * lap_score)

    if grad_energy < 20.0 or lap < 2.0:
        issues.append("blurred_image")
    return _clip(score), issues


def _text_contamination_score(rgb: np.ndarray) -> tuple[float, list[str]]:
    issues: list[str] = []
    gray = rgb.astype(np.float32).mean(axis=2)
    if gray.size == 0:
        return 0.0, ["empty_image"]

    dx = np.abs(np.diff(gray, axis=1))
    dy = np.abs(np.diff(gray, axis=0))
    edge_map = np.pad(dx, ((0, 0), (0, 1)), mode="constant") + np.pad(dy, ((0, 1), (0, 0)), mode="constant")
    threshold = float(np.percentile(edge_map, 96))
    binary = (edge_map >= threshold).astype(np.uint8)

    neighbors = (
        np.pad(binary, 1)[1:-1, :-2]
        + np.pad(binary, 1)[1:-1, 2:]
        + np.pad(binary, 1)[:-2, 1:-1]
        + np.pad(binary, 1)[2:, 1:-1]
    )
    tiny_ratio = float(((binary == 1) & (neighbors <= 2)).mean())

    # OCR-like horizontal/vertical stroke proxy.
    horiz = np.abs(np.diff(gray, axis=1))
    vert = np.abs(np.diff(gray, axis=0))
    horiz_stroke = float((horiz > np.percentile(horiz, 97)).mean())
    vert_stroke = float((vert > np.percentile(vert, 97)).mean())
    stroke_ratio = (horiz_stroke + vert_stroke) / 2.0

    contamination = (0.70 * _clip((tiny_ratio - 0.012) / 0.055)) + (0.30 * _clip((stroke_ratio - 0.025) / 0.11))
    score = 1.0 - contamination

    if tiny_ratio > 0.03:
        issues.append("text_contamination_risk")
    if stroke_ratio > 0.10:
        issues.append("stroke_artifact_risk")

    return _clip(score), issues


def _border_safety_score(alpha: np.ndarray) -> tuple[float, list[str]]:
    issues: list[str] = []
    if alpha.size == 0:
        return 0.0, ["empty_alpha"]

    h, w = alpha.shape[:2]
    center_x = (w - 1) / 2.0
    center_y = (h - 1) / 2.0
    radius = min(w, h) / 2.0
    yy, xx = np.ogrid[:h, :w]
    dist = np.sqrt((xx - center_x) ** 2 + (yy - center_y) ** 2)

    outer_ring = dist >= (radius - 3.5)
    outer_alpha = alpha[outer_ring] / 255.0
    bleed_ratio = float((outer_alpha > 0.05).mean()) if outer_alpha.size else 0.0

    score = 1.0 - _clip((bleed_ratio - 0.01) / 0.35)
    if bleed_ratio > 0.08:
        issues.append("border_bleed_risk")
    return _clip(score), issues


def _prompt_relevance_score(*, prompt: str, rgb: np.ndarray) -> tuple[float, list[str]]:
    issues: list[str] = []
    token = str(prompt or "").strip().lower()
    if not token:
        return 0.75, ["missing_prompt_context"]

    arr = rgb.astype(np.float32)
    r = arr[..., 0]
    g = arr[..., 1]
    b = arr[..., 2]
    brightness = (r + g + b) / 3.0

    score = 0.70

    maritime_words = {"sea", "ocean", "whale", "ship", "naval", "wave", "storm", "harbor"}
    gothic_words = {"gothic", "night", "shadow", "castle", "vampire", "dracula"}
    fire_words = {"golden", "sunset", "fire", "warm", "candle", "torch"}

    if any(word in token for word in maritime_words):
        blue_ratio = float(((b > r) & (b > g)).mean())
        maritime_score = _clip((blue_ratio - 0.16) / 0.52)
        score = (0.40 * score) + (0.60 * maritime_score)
        if blue_ratio < 0.12:
            issues.append("prompt_relevance_marine_mismatch")
    elif any(word in token for word in gothic_words):
        dark_ratio = float((brightness < 78.0).mean())
        gothic_score = _clip((dark_ratio - 0.25) / 0.60)
        score = (0.40 * score) + (0.60 * gothic_score)
        if dark_ratio < 0.22:
            issues.append("prompt_relevance_tone_mismatch")
    elif any(word in token for word in fire_words):
        warm_ratio = float(((r > g) & (g >= b)).mean())
        warm_score = _clip((warm_ratio - 0.18) / 0.60)
        score = (0.40 * score) + (0.60 * warm_score)
        if warm_ratio < 0.15:
            issues.append("prompt_relevance_warmth_mismatch")

    # Penalize very low visual complexity for narrative prompts.
    narrative_words = {"battle", "scene", "portrait", "journey", "adventure", "epic"}
    if any(word in token for word in narrative_words):
        edge_energy = float(np.abs(np.diff(brightness, axis=1)).mean() + np.abs(np.diff(brightness, axis=0)).mean())
        complexity = _clip((edge_energy - 4.0) / 18.0)
        score = (0.75 * score) + (0.25 * complexity)
        if complexity < 0.25:
            issues.append("prompt_relevance_low_complexity")

    return _clip(score), issues


def _artifact_score(rgb: np.ndarray) -> tuple[float, list[str]]:
    issues: list[str] = []
    gray = rgb.astype(np.float32).mean(axis=2)

    dx = np.abs(np.diff(gray, axis=1))
    dy = np.abs(np.diff(gray, axis=0))
    edge_map = np.pad(dx, ((0, 0), (0, 1)), mode="constant") + np.pad(dy, ((0, 1), (0, 0)), mode="constant")

    high_edges = edge_map > np.percentile(edge_map, 92)
    edge_ratio = float(high_edges.mean())

    # Text-like artifact proxy: too many tiny high-frequency islands.
    binary = high_edges.astype(np.uint8)
    neighbors = (
        np.pad(binary, 1)[1:-1, :-2]
        + np.pad(binary, 1)[1:-1, 2:]
        + np.pad(binary, 1)[:-2, 1:-1]
        + np.pad(binary, 1)[2:, 1:-1]
    )
    isolated_ratio = float(((binary == 1) & (neighbors <= 1)).mean())

    # Deformation proxy: abnormal channel disagreement spikes.
    rg_diff = np.abs(rgb[..., 0].astype(np.int16) - rgb[..., 1].astype(np.int16))
    gb_diff = np.abs(rgb[..., 1].astype(np.int16) - rgb[..., 2].astype(np.int16))
    chroma_outlier_ratio = float(((rg_diff > 120) | (gb_diff > 120)).mean())

    if isolated_ratio > 0.03:
        issues.append("text_like_artifacts")
    if chroma_outlier_ratio > 0.08:
        issues.append("distorted_color_artifacts")
    if edge_ratio < 0.004:
        issues.append("low_detail")

    artifact_penalty = 0.0
    artifact_penalty += 0.55 * _clip((isolated_ratio - 0.01) / 0.06)
    artifact_penalty += 0.30 * _clip((chroma_outlier_ratio - 0.02) / 0.16)
    artifact_penalty += 0.75 * _clip((0.010 - edge_ratio) / 0.010)
    artifact_penalty += 0.20 * _clip((edge_ratio - 0.20) / 0.30)

    score = 1.0 - artifact_penalty
    return _clip(score), issues


def _distinctiveness_score(*, image_path: Path, book_number: int) -> float:
    runtime = config.get_config()
    catalog_id = getattr(runtime, "catalog_id", None)
    try:
        result = similarity_detector.check_generated_image_against_winners(
            image_path=image_path,
            book_number=book_number,
            output_dir=runtime.output_dir,
            catalog_path=runtime.book_catalog_path,
            winner_selections_path=config.winner_selections_path(catalog_id=catalog_id, data_dir=runtime.data_dir),
            regions_path=config.cover_regions_path(catalog_id=catalog_id, config_dir=runtime.config_dir),
            hashes_path=config.similarity_hashes_path(catalog_id=catalog_id, data_dir=runtime.data_dir),
            threshold=0.25,
        )
        distance = _clip(float(result.get("similarity", 1.0) or 1.0))
        # similarity_detector uses a distance-like score where lower values mean more similar.
        # Convert to a quality-oriented distinctiveness signal that only penalizes near-duplicates.
        if distance >= 0.40:
            return 1.0
        if distance >= 0.25:
            return _clip(0.80 + ((distance - 0.25) / 0.15) * 0.20)
        if distance >= 0.15:
            return _clip(0.50 + ((distance - 0.15) / 0.10) * 0.30)
        if distance >= 0.08:
            return _clip(0.20 + ((distance - 0.08) / 0.07) * 0.30)
        return 0.0
    except Exception:  # pragma: no cover - defensive
        return 1.0


def _normalize_model_key(model: str) -> str:
    token = str(model or "").strip().lower()
    if "__" in token:
        token = token.replace("__", "/")
    return token


def _build_prompt_lookup() -> dict[tuple[int, int, str], str]:
    runtime = config.get_config()
    lookup: dict[tuple[int, int, str], str] = {}
    catalog_id = getattr(runtime, "catalog_id", None)
    data_dir = getattr(runtime, "data_dir", None)

    history_payload = _load_json(
        config.generation_history_path(catalog_id=catalog_id, data_dir=data_dir),
        {"items": []},
    )
    rows = history_payload.get("items", []) if isinstance(history_payload, dict) else []
    if not isinstance(rows, list):
        rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        book = _safe_int(row.get("book_number"), 0)
        variant = _safe_int(row.get("variant"), 0)
        model = _normalize_model_key(str(row.get("model", "default")))
        prompt = str(row.get("prompt", "")).strip()
        if book <= 0 or variant <= 0 or not prompt:
            continue
        lookup[(book, variant, model)] = prompt
        if model != "default":
            lookup.setdefault((book, variant, "default"), prompt)

    prompts_payload = _load_json(runtime.prompts_path, {"books": []})
    books = prompts_payload.get("books", []) if isinstance(prompts_payload, dict) else []
    if isinstance(books, list):
        for book_row in books:
            if not isinstance(book_row, dict):
                continue
            book = _safe_int(book_row.get("number"), 0)
            if book <= 0:
                continue
            variants = book_row.get("variants", [])
            if not isinstance(variants, list):
                continue
            for variant_row in variants:
                if not isinstance(variant_row, dict):
                    continue
                variant = _safe_int(variant_row.get("variant_id"), 0)
                prompt = str(variant_row.get("prompt", "")).strip()
                if variant <= 0 or not prompt:
                    continue
                lookup.setdefault((book, variant, "default"), prompt)
    return lookup


def _resolve_candidate_prompt(
    *,
    prompt_lookup: dict[tuple[int, int, str], str],
    book_number: int,
    variant_id: int,
    model: str,
) -> str:
    normalized_model = _normalize_model_key(model)
    return (
        prompt_lookup.get((book_number, variant_id, normalized_model))
        or prompt_lookup.get((book_number, variant_id, "default"))
        or ""
    )


def _apply_diversity_scores(scores: list[QualityScore], *, threshold: float) -> None:
    grouped: dict[tuple[int, str], list[QualityScore]] = {}
    for item in scores:
        grouped.setdefault((item.book_number, item.model), []).append(item)

    for key, entries in grouped.items():
        if len(entries) < 2:
            continue

        hashes = [
            _average_hash(Image.open(entry.image_path).convert("RGB"))
            for entry in entries
        ]
        dists: list[float] = []
        for i in range(len(hashes)):
            for j in range(i + 1, len(hashes)):
                dists.append(_hamming_distance(hashes[i], hashes[j]) / len(hashes[i]))

        avg_dist = float(np.mean(dists)) if dists else 1.0
        diversity_score = _clip(avg_dist / 0.35)

        identical = diversity_score < 0.25
        for entry in entries:
            entry.diversity_score = round(diversity_score, 4)
            if identical and "not_diverse" not in entry.issues:
                entry.issues.append("not_diverse")
            entry.passed = entry.overall_score >= threshold
            if not entry.passed and entry.recommendation == "accept":
                entry.recommendation = "regenerate"


def _retry_failed_images(
    *,
    scores: list[QualityScore],
    prompts_path: Path,
    threshold: float,
    max_retries: int,
) -> tuple[list[QualityScore], list[dict[str, Any]]]:
    retry_log: list[dict[str, Any]] = []
    prompts_payload = _load_prompts(prompts_path)
    runtime = config.get_config()

    for score in scores:
        if score.passed:
            continue

        original = _get_prompt_context(prompts_payload, score.book_number, score.variant_id)
        prompt = original["prompt"]
        negative_prompt = original["negative_prompt"]

        if not prompt:
            score.recommendation = "manual_review"
            score.issues.append("missing_prompt_context")
            continue

        for retry_idx in range(1, max_retries + 1):
            tweaked_prompt, tweaked_negative = _tweak_prompt(prompt, negative_prompt, retry_idx)
            try:
                image_bytes = image_generator.generate_image(
                    prompt=tweaked_prompt,
                    negative_prompt=tweaked_negative,
                    model=score.model,
                    params={
                        "provider": runtime.resolve_model_provider(score.model),
                        "width": runtime.image_width,
                        "height": runtime.image_height,
                        "allow_synthetic_fallback": True,
                    },
                )
                score.image_path.write_bytes(image_bytes)
                rescored = score_image(
                    score.image_path,
                    threshold=threshold,
                    book_number=score.book_number,
                    variant_id=score.variant_id,
                    model=score.model,
                    distinctiveness_score=_distinctiveness_score(
                        image_path=score.image_path,
                        book_number=score.book_number,
                    ),
                    prompt=tweaked_prompt,
                )

                retry_log.append(
                    {
                        "book_number": score.book_number,
                        "variant_id": score.variant_id,
                        "model": score.model,
                        "attempt": retry_idx,
                        "original_prompt": prompt,
                        "original_negative_prompt": negative_prompt,
                        "tweaked_prompt": tweaked_prompt,
                        "tweaked_negative_prompt": tweaked_negative,
                        "score_before": score.overall_score,
                        "score_after": rescored.overall_score,
                        "passed_after": rescored.passed,
                    }
                )

                score.technical_score = rescored.technical_score
                score.color_score = rescored.color_score
                score.palette_score = rescored.palette_score
                score.artifact_score = rescored.artifact_score
                score.blur_score = rescored.blur_score
                score.text_contamination_score = rescored.text_contamination_score
                score.border_safety_score = rescored.border_safety_score
                score.prompt_relevance_score = rescored.prompt_relevance_score
                score.overall_score = rescored.overall_score
                score.issues = rescored.issues
                score.passed = rescored.passed
                score.retries = retry_idx
                score.recommendation = "accept" if score.passed else "regenerate"

                if score.passed:
                    break
            except Exception as exc:  # pragma: no cover - defensive
                retry_log.append(
                    {
                        "book_number": score.book_number,
                        "variant_id": score.variant_id,
                        "model": score.model,
                        "attempt": retry_idx,
                        "original_prompt": prompt,
                        "original_negative_prompt": negative_prompt,
                        "tweaked_prompt": tweaked_prompt,
                        "tweaked_negative_prompt": tweaked_negative,
                        "error": str(exc),
                    }
                )

        if not score.passed:
            score.recommendation = "manual_review"
            if "retry_exhausted" not in score.issues:
                score.issues.append("retry_exhausted")

    _apply_diversity_scores(scores, threshold=threshold)
    return scores, retry_log


def _tweak_prompt(prompt: str, negative_prompt: str, attempt: int) -> tuple[str, str]:
    attempt_mods = {
        1: "enhance anatomical coherence, stronger composition focus, cleaner silhouettes",
        2: "richer brush texture, clearer focal subject, disciplined lighting hierarchy",
        3: "museum-quality draftsmanship, remove visual clutter, emphasize narrative clarity",
    }
    neg_mods = {
        1: "text artifacts, malformed hands, duplicate limbs",
        2: "garbled typography, warped faces, noisy textures",
        3: "AI glitches, repeated motifs, smudged features",
    }

    prompt_add = attempt_mods.get(attempt, attempt_mods[3])
    neg_add = neg_mods.get(attempt, neg_mods[3])

    tweaked_prompt = f"{prompt}, {prompt_add}"
    tweaked_negative = f"{negative_prompt}, {neg_add}" if negative_prompt else neg_add
    return tweaked_prompt, tweaked_negative


def _get_prompt_context(prompts_payload: dict[str, Any], book_number: int, variant_id: int) -> dict[str, str]:
    for book in prompts_payload.get("books", []):
        if int(book.get("number", 0)) != int(book_number):
            continue

        variants = book.get("variants", [])
        for variant in variants:
            if int(variant.get("variant_id", 0)) == int(variant_id):
                return {
                    "prompt": str(variant.get("prompt", "")),
                    "negative_prompt": str(variant.get("negative_prompt", "")),
                }

        # Fallback when generated variant id is > available prompt variants.
        if variants:
            first = variants[0]
            return {
                "prompt": str(first.get("prompt", "")),
                "negative_prompt": str(first.get("negative_prompt", "")),
            }

    return {"prompt": "", "negative_prompt": ""}


def _load_prompts(path: Path) -> dict[str, Any]:
    payload = safe_json.load_json(path, {"books": []})
    return payload if isinstance(payload, dict) else {"books": []}


def _load_json(path: Path, default: Any) -> Any:
    return safe_json.load_json(path, default)


def _collect_generated_images(image_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    if not image_dir.exists():
        return records

    # Pattern A: tmp/generated/{book}/{model}/variant_{n}.png
    for file_path in sorted(image_dir.glob("*/*/variant_*.png")):
        try:
            book_number = int(file_path.parents[1].name)
        except ValueError:
            continue
        model = file_path.parent.name
        variant = _parse_variant_id(file_path.stem)
        records.append(
            {
                "book_number": book_number,
                "variant_id": variant,
                "model": model,
                "image_path": file_path,
            }
        )

    # Pattern B: tmp/generated/{book}/variant_{n}.png
    for file_path in sorted(image_dir.glob("*/variant_*.png")):
        if file_path.parent.name == "history":
            continue

        parent = file_path.parent
        if parent.parent == image_dir:
            try:
                book_number = int(parent.name)
            except ValueError:
                continue
            variant = _parse_variant_id(file_path.stem)
            records.append(
                {
                    "book_number": book_number,
                    "variant_id": variant,
                    "model": "default",
                    "image_path": file_path,
                }
            )

    # De-duplicate records by path.
    dedup: dict[str, dict[str, Any]] = {}
    for row in records:
        dedup[str(row["image_path"])] = row

    return sorted(dedup.values(), key=lambda row: (row["book_number"], row["model"], row["variant_id"]))


def _parse_variant_id(stem: str) -> int:
    if "variant_" in stem:
        tail = stem.split("variant_", 1)[1]
        token = tail.split("_", 1)[0]
        try:
            return int(token)
        except ValueError:
            return 0
    return 0


def _average_hash(image: Image.Image, size: int = 16) -> np.ndarray:
    tiny = image.convert("L").resize((size, size), Image.LANCZOS)
    arr = np.array(tiny, dtype=np.float32)
    return arr > float(arr.mean())


def _hamming_distance(a: np.ndarray, b: np.ndarray) -> int:
    return int(np.sum(a != b))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _clip(value: float) -> float:
    return float(max(0.0, min(1.0, value)))


def _write_quality_scores(scores: list[QualityScore], path: Path) -> None:
    existing_rows_by_key: dict[tuple[int, int, str], dict[str, Any]] = {}
    existing_history: dict[tuple[int, int, str], list[dict[str, Any]]] = {}
    existing_payload = safe_json.load_json(path, {})
    existing_rows = existing_payload.get("scores", []) if isinstance(existing_payload, dict) else []
    if isinstance(existing_rows, list):
        for row in existing_rows:
            if not isinstance(row, dict):
                continue
            try:
                key = (
                    int(row.get("book_number", 0) or 0),
                    int(row.get("variant_id", 0) or 0),
                    str(row.get("model", "unknown")),
                )
            except (TypeError, ValueError):
                continue
            existing_rows_by_key[key] = dict(row)
            history = row.get("history")
            if isinstance(history, list):
                existing_history[key] = [item for item in history if isinstance(item, dict)]

    today = datetime.now(timezone.utc).date().isoformat()
    updated_keys: set[tuple[int, int, str]] = set()
    for row in scores:
        item = row.to_dict()
        key = (int(item.get("book_number", 0)), int(item.get("variant_id", 0)), str(item.get("model", "unknown")))
        history = list(existing_history.get(key, []))
        if not history:
            history = [
                {
                    "date": today,
                    "action": "initial_generation",
                    "best_score": round(float(item.get("overall_score", 0.0) or 0.0), 4),
                }
            ]
        elif not any(str(entry.get("action", "")).strip().lower() == "initial_generation" for entry in history):
            history.insert(
                0,
                {
                    "date": today,
                    "action": "initial_generation",
                    "best_score": round(float(item.get("overall_score", 0.0) or 0.0), 4),
                },
            )

        item["history"] = history
        existing_rows_by_key[key] = item
        updated_keys.add(key)

    serialized_scores = sorted(
        [value for value in existing_rows_by_key.values() if isinstance(value, dict)],
        key=lambda item: (
            int(item.get("book_number", 0) or 0),
            str(item.get("model", "unknown")),
            int(item.get("variant_id", 0) or 0),
        ),
    )

    payload = {
        "summary": {
            "total": len(serialized_scores),
            "passed": sum(1 for row in serialized_scores if bool(row.get("passed", False))),
            "failed": sum(1 for row in serialized_scores if not bool(row.get("passed", False))),
            "manual_review": sum(1 for row in serialized_scores if str(row.get("recommendation", "")) == "manual_review"),
            "updated_rows": len(updated_keys),
        },
        "scores": serialized_scores,
    }
    safe_json.atomic_write_json(path, payload)


def _write_retry_log(rows: list[dict[str, Any]], path: Path) -> None:
    payload = {
        "retry_count": len(rows),
        "items": rows,
    }
    safe_json.atomic_write_json(path, payload)


def _write_model_rankings(rows: list[dict[str, Any]], path: Path) -> None:
    payload = {"rankings": rows}
    safe_json.atomic_write_json(path, payload)


def main() -> int:
    parser = argparse.ArgumentParser(description="Prompt 2B quality gate")
    parser.add_argument("--generated-dir", type=Path, default=config.TMP_DIR / "generated")
    parser.add_argument("--prompts-path", type=Path, default=config.PROMPTS_PATH)
    parser.add_argument("--threshold", type=float, default=0.7)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--no-retries", action="store_true")

    args = parser.parse_args()

    scores = run_quality_gate(
        generated_dir=args.generated_dir,
        prompts_path=args.prompts_path,
        threshold=args.threshold,
        max_retries=args.max_retries,
        perform_retries=not args.no_retries,
    )

    total = len(scores)
    passed = sum(1 for row in scores if row.passed)
    logger.info("Quality gate complete: %d/%d passed (threshold=%.2f)", passed, total, args.threshold)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
