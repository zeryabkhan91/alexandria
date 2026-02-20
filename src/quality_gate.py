"""Prompt 2B quality gate: score, filter, retry, and rank generated images."""

from __future__ import annotations

import argparse
import json
import logging
import statistics
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

try:
    from src import config
    from src import image_generator
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import image_generator  # type: ignore


logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@dataclass(slots=True)
class QualityScore:
    """Quality assessment for a single generated image."""

    book_number: int
    variant_id: int
    model: str
    image_path: Path
    technical_score: float
    color_score: float
    artifact_score: float
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
) -> QualityScore:
    """Score a single generated image."""
    image = Image.open(image_path).convert("RGB")
    rgb = np.array(image, dtype=np.uint8)

    technical_score, technical_issues = _technical_quality(rgb)
    color_score, color_issues = _color_compatibility(rgb)
    artifact_score, artifact_issues = _artifact_score(rgb)

    issues = technical_issues + color_issues + artifact_issues
    overall = (0.45 * technical_score) + (0.25 * color_score) + (0.30 * artifact_score)

    passed = overall >= threshold
    recommendation = "accept" if passed else "regenerate"

    return QualityScore(
        book_number=book_number,
        variant_id=variant_id,
        model=model,
        image_path=image_path,
        technical_score=round(technical_score, 4),
        color_score=round(color_score, 4),
        artifact_score=round(artifact_score, 4),
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
    scores: list[QualityScore] = []

    for item in candidates:
        score = score_image(
            item["image_path"],
            threshold=threshold,
            book_number=item["book_number"],
            variant_id=item["variant_id"],
            model=item["model"],
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
    output_scores_path = output_scores_path or (config.DATA_DIR / "quality_scores.json")
    output_report_path = output_report_path or (config.DATA_DIR / "quality_report.md")
    retry_log_path = retry_log_path or (config.DATA_DIR / "retry_log.json")
    model_rankings_path = model_rankings_path or (config.DATA_DIR / "model_rankings.json")

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
        avg_artifact = statistics.mean(item.artifact_score for item in items) if items else 0.0

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
                "average_artifact": round(avg_artifact, 4),
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

    ratio_score = 1.0 - abs(warm_ratio - 0.35) / 0.65
    ratio_score = _clip(ratio_score)

    if warm_ratio < 0.08:
        issues.append("too_cold_for_cover_palette")
    if warm_ratio > 0.93:
        issues.append("overly_monochrome_warm")

    score = (0.60 * ratio_score) + (0.40 * warm_balance_score)
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
            entry.overall_score = round(entry.overall_score * (0.80 + 0.20 * diversity_score), 4)
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
                score.artifact_score = rescored.artifact_score
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
    if not path.exists():
        return {"books": []}
    return json.loads(path.read_text(encoding="utf-8"))


def _collect_generated_images(image_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    if not image_dir.exists():
        return records

    # Pattern A: tmp/generated/{book}/{model}/variant_{n}.png
    for file_path in sorted(image_dir.glob("*/ */variant_*.png")):
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


def _clip(value: float) -> float:
    return float(max(0.0, min(1.0, value)))


def _write_quality_scores(scores: list[QualityScore], path: Path) -> None:
    payload = {
        "summary": {
            "total": len(scores),
            "passed": sum(1 for row in scores if row.passed),
            "failed": sum(1 for row in scores if not row.passed),
            "manual_review": sum(1 for row in scores if row.recommendation == "manual_review"),
        },
        "scores": [row.to_dict() for row in scores],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _write_retry_log(rows: list[dict[str, Any]], path: Path) -> None:
    payload = {
        "retry_count": len(rows),
        "items": rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _write_model_rankings(rows: list[dict[str, Any]], path: Path) -> None:
    payload = {"rankings": rows}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


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
