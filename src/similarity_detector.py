"""Cross-book visual similarity detection and duplicate prevention (Prompt 11B)."""

from __future__ import annotations

import argparse
import math
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageDraw

try:
    import cv2  # type: ignore
except Exception:  # pragma: no cover - optional fallback
    cv2 = None

try:
    from src import config
    from src import safe_json
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import safe_json  # type: ignore
    from logger import get_logger  # type: ignore

logger = get_logger(__name__)

DEFAULT_HASHES_PATH = config.similarity_hashes_path()
DEFAULT_MATRIX_PATH = config.similarity_matrix_path()
DEFAULT_CLUSTERS_PATH = config.similarity_clusters_path()
DEFAULT_DISMISSED_PATH = config.similarity_dismissed_path()
DEFAULT_REPORT_PATH = config.DATA_DIR / "similarity_report.html"

# Exhaustive comparison is precise but expensive. Above this threshold we switch
# to LSH candidate generation to keep similarity checks bounded at scale.
EXHAUSTIVE_PAIR_LIMIT = 500
LSH_BANDS = 8
LSH_BAND_BITS = 8


@dataclass(slots=True)
class CoverHashes:
    book_number: int
    image_path: str
    phash: str
    dhash: str
    color_hist: list[float]
    dominant_colors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "book_number": self.book_number,
            "image_path": self.image_path,
            "phash": self.phash,
            "dhash": self.dhash,
            "color_hist": [round(float(v), 8) for v in self.color_hist],
            "dominant_colors": self.dominant_colors,
        }


def run_similarity_analysis(
    *,
    output_dir: Path,
    threshold: float = 0.25,
    catalog_path: Path | None = None,
    winner_selections_path: Path | None = None,
    regions_path: Path | None = None,
    hashes_path: Path | None = None,
    matrix_path: Path | None = None,
    clusters_path: Path | None = None,
    workers: int = 8,
) -> dict[str, Any]:
    runtime = config.get_config()
    catalog_id = getattr(runtime, "catalog_id", None)
    catalog_path = catalog_path or runtime.book_catalog_path
    winner_selections_path = winner_selections_path or config.winner_selections_path(
        catalog_id=catalog_id,
        data_dir=runtime.data_dir,
    )
    regions_path = regions_path or config.cover_regions_path(catalog_id=catalog_id, config_dir=runtime.config_dir)
    hashes_path = hashes_path or config.similarity_hashes_path(catalog_id=catalog_id, data_dir=runtime.data_dir)
    matrix_path = matrix_path or config.similarity_matrix_path(catalog_id=catalog_id, data_dir=runtime.data_dir)
    clusters_path = clusters_path or config.similarity_clusters_path(catalog_id=catalog_id, data_dir=runtime.data_dir)

    winners = _load_winner_cover_paths(
        output_dir=output_dir,
        catalog_path=catalog_path,
        winner_selections_path=winner_selections_path,
    )
    regions = _load_region_config(regions_path)

    hashes = compute_cover_hashes(
        winners=winners,
        regions=regions,
        workers=workers,
    )

    hashes_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "books": {f"book_{book}": value.to_dict() for book, value in sorted(hashes.items())},
        "count": len(hashes),
    }

    matrix_payload = build_similarity_matrix(hashes=hashes, threshold=threshold)

    clusters_payload = detect_clusters(
        pairs=matrix_payload.get("pairs", []),
        threshold=0.35,
    )
    safe_json.atomic_write_many_json(
        [
            (hashes_path, hashes_payload),
            (matrix_path, matrix_payload),
            (clusters_path, clusters_payload),
        ]
    )

    summary = {
        "hashes": str(hashes_path),
        "matrix": str(matrix_path),
        "clusters": str(clusters_path),
        "books": len(hashes),
        "pairs": matrix_payload.get("total_pairs", 0),
        "alerts": matrix_payload.get("alerts", 0),
        "threshold": threshold,
    }
    logger.info(
        "Similarity analysis complete: %s books, %s pairs, %s alerts",
        summary["books"],
        summary["pairs"],
        summary["alerts"],
    )
    return summary


def compute_cover_hashes(
    *,
    winners: dict[int, Path],
    regions: dict[str, Any],
    workers: int = 8,
) -> dict[int, CoverHashes]:
    items = sorted(winners.items())
    if not items:
        return {}

    out: dict[int, CoverHashes] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        future_map = {
            pool.submit(_compute_hash_for_book, book_number=book, image_path=path, regions=regions): book
            for book, path in items
        }
        for future in as_completed(future_map):
            book = future_map[future]
            try:
                result = future.result()
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Failed hash for book %s: %s", book, exc)
                continue
            out[book] = result
    return out


def build_similarity_matrix(
    *,
    hashes: dict[int, CoverHashes],
    threshold: float = 0.25,
    mode: str = "auto",
) -> dict[str, Any]:
    books = sorted(hashes.keys())
    pairs: list[dict[str, Any]] = []
    pair_candidates = _candidate_book_pairs(hashes=hashes, mode=mode)

    for book_a, book_b in pair_candidates:
        hash_a = hashes.get(book_a)
        hash_b = hashes.get(book_b)
        if hash_a is None or hash_b is None:
            continue
        pairs.append(_pair_row(book_a=book_a, book_b=book_b, hash_a=hash_a, hash_b=hash_b, threshold=threshold))

    alerts = sum(1 for row in pairs if bool(row.get("alert")))
    comparison_mode = "exhaustive" if len(books) <= EXHAUSTIVE_PAIR_LIMIT and mode in {"auto", "exhaustive"} else "lsh"
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "alert_threshold": threshold,
        "total_pairs": len(pairs),
        "alerts": alerts,
        "book_count": len(books),
        "candidate_pairs": len(pair_candidates),
        "comparison_mode": comparison_mode,
        "pairs": pairs,
    }


def _pair_row(
    *,
    book_a: int,
    book_b: int,
    hash_a: CoverHashes,
    hash_b: CoverHashes,
    threshold: float,
) -> dict[str, Any]:
    metrics = _compare_hash_objects(hash_a, hash_b)
    similarity = metrics["combined_similarity"]
    alert = similarity < threshold
    reason = _build_alert_reason(hash_a, hash_b, metrics) if alert else ""
    return {
        "book_a": int(book_a),
        "book_b": int(book_b),
        "similarity": round(similarity, 4),
        "alert": alert,
        "reason": reason,
        "metrics": {
            "structural": round(metrics["structural_similarity"], 4),
            "edge": round(metrics["edge_similarity"], 4),
            "color": round(metrics["color_similarity"], 4),
        },
    }


def _candidate_book_pairs(*, hashes: dict[int, CoverHashes], mode: str = "auto") -> list[tuple[int, int]]:
    books = sorted(hashes.keys())
    if len(books) < 2:
        return []

    mode_token = str(mode or "auto").strip().lower()
    use_exhaustive = mode_token == "exhaustive" or (mode_token == "auto" and len(books) <= EXHAUSTIVE_PAIR_LIMIT)
    if use_exhaustive:
        return [(book_a, book_b) for idx, book_a in enumerate(books) for book_b in books[idx + 1 :]]

    buckets: dict[str, list[int]] = defaultdict(list)
    for book in books:
        row = hashes[book]
        for key in _lsh_bucket_keys(row.phash):
            buckets[key].append(book)

    pairs: set[tuple[int, int]] = set()
    for rows in buckets.values():
        if len(rows) < 2:
            continue
        unique_rows = sorted(set(rows))
        if len(unique_rows) > 400:
            # Keep bucket comparisons bounded under pathological collisions.
            unique_rows = unique_rows[:400]
        for idx, book_a in enumerate(unique_rows):
            for book_b in unique_rows[idx + 1 :]:
                pairs.add((book_a, book_b))

    if not pairs:
        # Fallback to exhaustive for small-ish sets if collisions were absent.
        if len(books) <= EXHAUSTIVE_PAIR_LIMIT * 2:
            return [(book_a, book_b) for idx, book_a in enumerate(books) for book_b in books[idx + 1 :]]
        # Last-resort fallback keeps at least neighboring comparisons.
        pairs = {(books[idx], books[idx + 1]) for idx in range(len(books) - 1)}

    return sorted(pairs)


def _lsh_bucket_keys(phash_hex: str, *, bands: int = LSH_BANDS, band_bits: int = LSH_BAND_BITS) -> list[str]:
    bits = _hex_to_bits(phash_hex)
    if not bits:
        return []
    total_bits = len(bits)
    per_band = max(1, min(total_bits, int(band_bits)))
    count = max(1, min(int(bands), max(1, total_bits // per_band)))
    keys: list[str] = []
    for idx in range(count):
        start = idx * per_band
        end = min(total_bits, start + per_band)
        if start >= total_bits:
            break
        token = "".join(str(bit) for bit in bits[start:end])
        keys.append(f"{idx}:{token}")
    return keys


def _parse_hash_payload(payload: dict[str, Any]) -> dict[int, CoverHashes]:
    books = payload.get("books", {}) if isinstance(payload, dict) else {}
    parsed: dict[int, CoverHashes] = {}
    if not isinstance(books, dict):
        return parsed
    for key, value in books.items():
        if not isinstance(value, dict):
            continue
        book = _safe_int(value.get("book_number"), 0)
        if book <= 0 and str(key).startswith("book_"):
            book = _safe_int(str(key).split("book_", 1)[1], 0)
        if book <= 0:
            continue
        row = _cover_hash_from_row(book=book, value=value)
        if row is not None:
            parsed[book] = row
    return parsed


def update_similarity_for_book(
    *,
    output_dir: Path,
    book_number: int,
    threshold: float = 0.25,
    catalog_path: Path | None = None,
    winner_selections_path: Path | None = None,
    regions_path: Path | None = None,
    hashes_path: Path | None = None,
    matrix_path: Path | None = None,
    clusters_path: Path | None = None,
) -> dict[str, Any]:
    runtime = config.get_config()
    catalog_id = getattr(runtime, "catalog_id", None)
    catalog_path = catalog_path or runtime.book_catalog_path
    winner_selections_path = winner_selections_path or config.winner_selections_path(
        catalog_id=catalog_id,
        data_dir=runtime.data_dir,
    )
    regions_path = regions_path or config.cover_regions_path(catalog_id=catalog_id, config_dir=runtime.config_dir)
    hashes_path = hashes_path or config.similarity_hashes_path(catalog_id=catalog_id, data_dir=runtime.data_dir)
    matrix_path = matrix_path or config.similarity_matrix_path(catalog_id=catalog_id, data_dir=runtime.data_dir)
    clusters_path = clusters_path or config.similarity_clusters_path(catalog_id=catalog_id, data_dir=runtime.data_dir)

    winners = _load_winner_cover_paths(
        output_dir=output_dir,
        catalog_path=catalog_path,
        winner_selections_path=winner_selections_path,
    )
    if int(book_number) not in winners:
        return {
            "ok": False,
            "book_number": int(book_number),
            "error": f"winner image for book {book_number} not found",
        }

    regions = _load_region_config(regions_path)
    existing_hashes_payload = _load_json_dict(hashes_path)
    parsed_hashes = _parse_hash_payload(existing_hashes_payload)

    # Keep only books that still have winner images.
    parsed_hashes = {book: row for book, row in parsed_hashes.items() if book in winners}
    parsed_hashes[int(book_number)] = _compute_hash_for_book(
        book_number=int(book_number),
        image_path=winners[int(book_number)],
        regions=regions,
    )

    matrix_payload = _load_json_dict(matrix_path)
    existing_pairs = matrix_payload.get("pairs", []) if isinstance(matrix_payload.get("pairs"), list) else []
    kept_pairs = [
        row
        for row in existing_pairs
        if isinstance(row, dict)
        and _safe_int(row.get("book_a"), 0) != int(book_number)
        and _safe_int(row.get("book_b"), 0) != int(book_number)
    ]

    book_hash = parsed_hashes[int(book_number)]
    updated_rows: list[dict[str, Any]] = []
    for other_book in sorted(parsed_hashes.keys()):
        if int(other_book) == int(book_number):
            continue
        other_hash = parsed_hashes[other_book]
        a, b = sorted((int(book_number), int(other_book)))
        row = _pair_row(
            book_a=a,
            book_b=b,
            hash_a=parsed_hashes[a],
            hash_b=parsed_hashes[b],
            threshold=threshold,
        )
        updated_rows.append(row)

    all_pairs = kept_pairs + updated_rows
    all_pairs.sort(key=lambda row: (_safe_int(row.get("book_a"), 0), _safe_int(row.get("book_b"), 0)))
    alerts = sum(1 for row in all_pairs if bool(row.get("alert")))

    hashes_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "books": {f"book_{book}": value.to_dict() for book, value in sorted(parsed_hashes.items())},
        "count": len(parsed_hashes),
    }
    new_matrix_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "alert_threshold": threshold,
        "total_pairs": len(all_pairs),
        "alerts": alerts,
        "book_count": len(parsed_hashes),
        "comparison_mode": "incremental",
        "candidate_pairs": len(all_pairs),
        "pairs": all_pairs,
    }
    clusters_payload = detect_clusters(
        pairs=all_pairs,
        threshold=0.35,
    )
    safe_json.atomic_write_many_json(
        [
            (hashes_path, hashes_payload),
            (matrix_path, new_matrix_payload),
            (clusters_path, clusters_payload),
        ]
    )
    return {
        "ok": True,
        "book_number": int(book_number),
        "compared_books": max(0, len(parsed_hashes) - 1),
        "total_pairs": len(all_pairs),
        "alerts": alerts,
        "hashes": str(hashes_path),
        "matrix": str(matrix_path),
        "clusters": str(clusters_path),
        "image_path": str(book_hash.image_path),
    }


def detect_clusters(*, pairs: list[dict[str, Any]], threshold: float = 0.35) -> dict[str, Any]:
    graph: dict[int, set[int]] = defaultdict(set)
    for row in pairs:
        if not isinstance(row, dict):
            continue
        similarity = _safe_float(row.get("similarity"), 1.0)
        if similarity >= threshold:
            continue
        a = _safe_int(row.get("book_a"), 0)
        b = _safe_int(row.get("book_b"), 0)
        if a <= 0 or b <= 0:
            continue
        graph[a].add(b)
        graph[b].add(a)

    visited: set[int] = set()
    clusters: list[dict[str, Any]] = []

    for node in sorted(graph.keys()):
        if node in visited:
            continue
        stack = [node]
        component: set[int] = set()
        while stack:
            cur = stack.pop()
            if cur in visited:
                continue
            visited.add(cur)
            component.add(cur)
            for nxt in graph.get(cur, set()):
                if nxt not in visited:
                    stack.append(nxt)

        if len(component) < 2:
            continue
        members = sorted(component)
        sims: list[float] = []
        for row in pairs:
            if not isinstance(row, dict):
                continue
            a = _safe_int(row.get("book_a"), 0)
            b = _safe_int(row.get("book_b"), 0)
            if a in component and b in component:
                sims.append(_safe_float(row.get("similarity"), 1.0))

        avg_similarity = float(sum(sims) / len(sims)) if sims else 1.0
        clusters.append(
            {
                "books": members,
                "theme": "similar medallion composition cluster",
                "avg_similarity": round(avg_similarity, 4),
            }
        )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "threshold": threshold,
        "clusters": clusters,
    }


def check_generated_image_against_winners(
    *,
    image_path: Path,
    book_number: int,
    output_dir: Path,
    catalog_path: Path,
    winner_selections_path: Path,
    regions_path: Path,
    hashes_path: Path | None = None,
    threshold: float = 0.25,
) -> dict[str, Any]:
    if hashes_path is None:
        runtime = config.get_config()
        hashes_path = config.similarity_hashes_path(
            catalog_id=getattr(runtime, "catalog_id", None),
            data_dir=getattr(runtime, "data_dir", None),
        )

    winners = _load_winner_cover_paths(
        output_dir=output_dir,
        catalog_path=catalog_path,
        winner_selections_path=winner_selections_path,
    )
    regions = _load_region_config(regions_path)
    cached_hashes = _load_or_build_winner_hashes(
        winners=winners,
        regions=regions,
        hashes_path=hashes_path,
    )

    probe = _compute_hash_for_book(book_number=book_number, image_path=image_path, regions=regions)

    closest_book = None
    closest_distance = 1.0
    closest_metrics: dict[str, float] = {
        "structural_similarity": 1.0,
        "edge_similarity": 1.0,
        "color_similarity": 1.0,
        "combined_similarity": 1.0,
    }

    for other_book, other_hash in cached_hashes.items():
        if int(other_book) == int(book_number):
            continue
        metrics = _compare_hash_objects(probe, other_hash)
        distance = metrics["combined_similarity"]
        if distance < closest_distance:
            closest_distance = distance
            closest_book = other_book
            closest_metrics = metrics

    return {
        "book_number": book_number,
        "closest_book": closest_book,
        "similarity": round(float(closest_distance), 4),
        "alert": bool(closest_book is not None and closest_distance < threshold),
        "reason": "Potential duplicate medallion style" if closest_book is not None and closest_distance < threshold else "",
        "metrics": {
            "structural": round(float(closest_metrics["structural_similarity"]), 4),
            "edge": round(float(closest_metrics["edge_similarity"]), 4),
            "color": round(float(closest_metrics["color_similarity"]), 4),
        },
    }


def check_prompt_similarity_against_winners(
    *,
    prompt: str,
    current_book: int,
    winner_selections_path: Path,
    generation_history_path: Path,
    threshold: float = 0.85,
) -> dict[str, Any]:
    winner_payload = _load_json_dict(winner_selections_path)
    selections = winner_payload.get("selections", winner_payload)
    if not isinstance(selections, dict):
        selections = {}

    history_payload = _load_json_dict(generation_history_path)
    items = history_payload.get("items", []) if isinstance(history_payload.get("items"), list) else []

    winner_variants: dict[int, int] = {}
    for key, value in selections.items():
        book = _safe_int(key, 0)
        if book <= 0:
            continue
        variant = _safe_int(value.get("winner") if isinstance(value, dict) else value, 0)
        if variant <= 0:
            continue
        winner_variants[book] = variant

    candidates: list[tuple[int, str]] = []
    for row in items:
        if not isinstance(row, dict):
            continue
        book = _safe_int(row.get("book_number"), 0)
        if book <= 0 or book == int(current_book):
            continue
        variant = _safe_int(row.get("variant", row.get("variant_id")), 0)
        winner_variant = winner_variants.get(book)
        if winner_variant and winner_variant != variant:
            continue
        existing_prompt = str(row.get("prompt", "")).strip()
        if existing_prompt:
            candidates.append((book, existing_prompt))

    best_book = None
    best_similarity = 0.0
    for book, existing in candidates:
        similarity = prompt_text_similarity(prompt, existing)
        if similarity > best_similarity:
            best_similarity = similarity
            best_book = book

    return {
        "similarity": round(best_similarity, 4),
        "closest_book": best_book,
        "alert": bool(best_similarity > threshold),
    }


def prompt_text_similarity(a: str, b: str) -> float:
    doc_a = _tokenize_text(a)
    doc_b = _tokenize_text(b)
    if not doc_a or not doc_b:
        return 0.0

    vocab = sorted(set(doc_a).union(doc_b))
    if not vocab:
        return 0.0

    tf_a = Counter(doc_a)
    tf_b = Counter(doc_b)
    df = {token: int(token in tf_a) + int(token in tf_b) for token in vocab}

    vec_a = []
    vec_b = []
    for token in vocab:
        idf = math.log((2 + 1) / (df[token] + 1)) + 1.0
        vec_a.append(float(tf_a.get(token, 0)) * idf)
        vec_b.append(float(tf_b.get(token, 0)) * idf)

    norm_a = math.sqrt(sum(value * value for value in vec_a))
    norm_b = math.sqrt(sum(value * value for value in vec_b))
    if norm_a <= 0 or norm_b <= 0:
        return 0.0

    dot = sum(x * y for x, y in zip(vec_a, vec_b))
    return float(max(0.0, min(1.0, dot / (norm_a * norm_b))))


def generate_report_html(
    *,
    matrix_path: Path,
    output_path: Path,
    output_dir: Path,
    catalog_path: Path,
    winner_selections_path: Path,
    threshold: float = 0.25,
) -> Path:
    matrix_payload = _load_json_dict(matrix_path)
    pairs = matrix_payload.get("pairs", []) if isinstance(matrix_payload.get("pairs"), list) else []
    alerts = [row for row in pairs if isinstance(row, dict) and _safe_float(row.get("similarity"), 1.0) < threshold]

    winners = _load_winner_cover_paths(
        output_dir=output_dir,
        catalog_path=catalog_path,
        winner_selections_path=winner_selections_path,
    )
    titles = _book_title_map(catalog_path)

    rows: list[str] = []
    for row in sorted(alerts, key=lambda item: _safe_float(item.get("similarity"), 1.0)):
        a = _safe_int(row.get("book_a"), 0)
        b = _safe_int(row.get("book_b"), 0)
        sim = _safe_float(row.get("similarity"), 1.0)
        image_a = winners.get(a)
        image_b = winners.get(b)
        if not image_a or not image_b:
            continue
        rel_a = _relative_or_str(image_a, output_dir.parent)
        rel_b = _relative_or_str(image_b, output_dir.parent)
        rows.append(
            "<tr>"
            f"<td>{a}. {titles.get(a, f'Book {a}')}</td>"
            f"<td>{b}. {titles.get(b, f'Book {b}')}</td>"
            f"<td>{sim:.3f}</td>"
            f"<td><img src='/{rel_a}' style='max-width:220px;border-radius:8px;'></td>"
            f"<td><img src='/{rel_b}' style='max-width:220px;border-radius:8px;'></td>"
            "</tr>"
        )

    html = (
        "<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<title>Similarity Report</title><style>"
        "body{font-family:Georgia,serif;background:#1a2744;color:#f5e6c8;padding:18px;}"
        "table{width:100%;border-collapse:collapse;background:#243454;}"
        "th,td{border:1px solid rgba(245,230,200,.25);padding:10px;vertical-align:top;}"
        "th{background:#1f2f52;color:#c4a352;text-align:left;}"
        "h1{color:#c4a352;}"
        "</style></head><body>"
        "<h1>Similarity Alert Report</h1>"
        f"<p>Threshold: {threshold:.2f}. Alerts: {len(rows)}.</p>"
        "<table><thead><tr><th>Book A</th><th>Book B</th><th>Similarity</th><th>A</th><th>B</th></tr></thead>"
        f"<tbody>{''.join(rows) if rows else '<tr><td colspan=5>No alerts</td></tr>'}</tbody></table>"
        "</body></html>"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return output_path


def _compute_hash_for_book(*, book_number: int, image_path: Path, regions: dict[str, Any]) -> CoverHashes:
    with Image.open(image_path) as image:
        rgb = image.convert("RGB")
        medallion = _extract_medallion_region(rgb, regions)
        phash_bits = _phash(medallion)
        dhash_bits = _dhash(medallion)
        color_hist = _color_histogram_hsv(medallion)
        dominant = _dominant_colors(medallion)

    return CoverHashes(
        book_number=book_number,
        image_path=str(image_path),
        phash=_bits_to_hex(phash_bits),
        dhash=_bits_to_hex(dhash_bits),
        color_hist=color_hist,
        dominant_colors=dominant,
    )


def _extract_medallion_region(image: Image.Image, regions: dict[str, Any]) -> Image.Image:
    consensus = regions.get("consensus_region", {}) if isinstance(regions, dict) else {}
    cover_size = regions.get("cover_size", {}) if isinstance(regions, dict) else {}
    base_w = _safe_int(cover_size.get("width"), image.width)
    base_h = _safe_int(cover_size.get("height"), image.height)
    scale_x = (image.width / base_w) if base_w > 0 else 1.0
    scale_y = (image.height / base_h) if base_h > 0 else 1.0

    cx = int(_safe_int(consensus.get("center_x"), image.width // 2) * scale_x)
    cy = int(_safe_int(consensus.get("center_y"), image.height // 2) * scale_y)
    radius = int(_safe_int(consensus.get("radius"), min(image.width, image.height) // 6) * min(scale_x, scale_y))
    if radius <= 0:
        radius = max(4, min(image.width, image.height) // 6)

    left = max(0, cx - radius)
    top = max(0, cy - radius)
    right = min(image.width, cx + radius)
    bottom = min(image.height, cy + radius)
    if right <= left or bottom <= top:
        cx = image.width // 2
        cy = image.height // 2
        radius = max(4, min(image.width, image.height) // 3)
        left = max(0, cx - radius)
        top = max(0, cy - radius)
        right = min(image.width, cx + radius)
        bottom = min(image.height, cy + radius)

    crop = image.crop((left, top, right, bottom))
    size = min(crop.width, crop.height)
    crop = crop.resize((size, size), Image.LANCZOS)

    mask = Image.new("L", (size, size), 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse((0, 0, size - 1, size - 1), fill=255)
    out = Image.new("RGB", (size, size), (0, 0, 0))
    out.paste(crop, mask=mask)
    return out


def _phash(image: Image.Image) -> np.ndarray:
    gray = np.array(image.convert("L").resize((32, 32), Image.LANCZOS), dtype=np.float32)
    if cv2 is not None:
        dct = cv2.dct(gray)
    else:  # pragma: no cover
        dct = np.fft.fft2(gray).real
    low = dct[:8, :8]
    med = np.median(low[1:, 1:]) if low.size else 0.0
    return (low > med).astype(np.uint8).flatten()


def _dhash(image: Image.Image) -> np.ndarray:
    gray = np.array(image.convert("L").resize((9, 8), Image.LANCZOS), dtype=np.float32)
    diff = gray[:, 1:] > gray[:, :-1]
    return diff.astype(np.uint8).flatten()


def _color_histogram_hsv(image: Image.Image) -> list[float]:
    arr = np.array(image.convert("RGB"), dtype=np.uint8)
    if cv2 is not None:
        hsv = cv2.cvtColor(arr, cv2.COLOR_RGB2HSV)
        hist_h = cv2.calcHist([hsv], [0], None, [32], [0, 180]).flatten()
        hist_s = cv2.calcHist([hsv], [1], None, [32], [0, 256]).flatten()
        hist_v = cv2.calcHist([hsv], [2], None, [32], [0, 256]).flatten()
    else:  # pragma: no cover
        hsv = np.array(image.convert("HSV"), dtype=np.uint8)
        hist_h, _ = np.histogram(hsv[..., 0], bins=32, range=(0, 255))
        hist_s, _ = np.histogram(hsv[..., 1], bins=32, range=(0, 255))
        hist_v, _ = np.histogram(hsv[..., 2], bins=32, range=(0, 255))
    hist = np.concatenate([hist_h, hist_s, hist_v]).astype(np.float64)
    total = float(hist.sum())
    if total > 0:
        hist /= total
    return hist.tolist()


def _dominant_colors(image: Image.Image, top_n: int = 3) -> list[str]:
    arr = np.array(image.convert("RGB"), dtype=np.uint8)
    flat = arr.reshape(-1, 3)
    if len(flat) == 0:
        return ["#000000"]
    bins = ((flat // 32) * 32).astype(np.uint8)
    keys = [tuple(pixel.tolist()) for pixel in bins]
    counts = Counter(keys)
    top = [color for color, _count in counts.most_common(top_n)]
    return [f"#{r:02X}{g:02X}{b:02X}" for r, g, b in top]


def _compare_hash_objects(a: CoverHashes, b: CoverHashes) -> dict[str, float]:
    ph_a = _hex_to_bits(a.phash)
    ph_b = _hex_to_bits(b.phash)
    dh_a = _hex_to_bits(a.dhash)
    dh_b = _hex_to_bits(b.dhash)

    structural = _hamming_distance(ph_a, ph_b) / max(1, len(ph_a))
    edge = _hamming_distance(dh_a, dh_b) / max(1, len(dh_a))
    color = _chi_squared_distance(np.array(a.color_hist), np.array(b.color_hist))
    color_normalized = color / (color + 1.0)

    combined = (0.4 * structural) + (0.3 * edge) + (0.3 * color_normalized)
    return {
        "structural_similarity": float(structural),
        "edge_similarity": float(edge),
        "color_similarity": float(color_normalized),
        "combined_similarity": float(max(0.0, min(1.0, combined))),
    }


def _build_alert_reason(a: CoverHashes, b: CoverHashes, metrics: dict[str, float]) -> str:
    color_overlap = set(a.dominant_colors).intersection(set(b.dominant_colors))
    if metrics["structural_similarity"] < 0.2 and metrics["edge_similarity"] < 0.2:
        return "Highly similar structure and edge composition"
    if color_overlap:
        return f"Similar palette overlap ({', '.join(sorted(color_overlap)[:2])})"
    return "Close visual composition across medallion region"


def _load_or_build_winner_hashes(
    *,
    winners: dict[int, Path],
    regions: dict[str, Any],
    hashes_path: Path,
) -> dict[int, CoverHashes]:
    expected_books = set(winners.keys())
    cached_payload = _load_json_dict(hashes_path)
    cached_books = cached_payload.get("books", {}) if isinstance(cached_payload, dict) else {}
    parsed: dict[int, CoverHashes] = {}

    if isinstance(cached_books, dict):
        for key, value in cached_books.items():
            if not isinstance(value, dict):
                continue
            book = _safe_int(value.get("book_number"), 0)
            if book <= 0 and str(key).startswith("book_"):
                book = _safe_int(str(key).split("book_", 1)[1], 0)
            if book <= 0:
                continue
            row = _cover_hash_from_row(book=book, value=value)
            if row is not None:
                parsed[book] = row

    if expected_books and expected_books.issubset(parsed.keys()):
        return {book: parsed[book] for book in expected_books}

    rebuilt = compute_cover_hashes(winners=winners, regions=regions, workers=8)
    if rebuilt:
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "books": {f"book_{book}": value.to_dict() for book, value in sorted(rebuilt.items())},
            "count": len(rebuilt),
        }
        safe_json.atomic_write_json(hashes_path, payload)
    return rebuilt


def _cover_hash_from_row(*, book: int, value: dict[str, Any]) -> CoverHashes | None:
    phash = str(value.get("phash", "")).strip()
    dhash = str(value.get("dhash", "")).strip()
    color_hist = value.get("color_hist", [])
    dominant_colors = value.get("dominant_colors", [])
    if not phash or not dhash:
        return None
    if not isinstance(color_hist, list):
        color_hist = []
    hist_values = [float(item) for item in color_hist if isinstance(item, (int, float))]
    if not hist_values:
        hist_values = [0.0] * 96
    if not isinstance(dominant_colors, list):
        dominant_colors = []
    dom = [str(item) for item in dominant_colors if str(item).strip()][:3]
    if not dom:
        dom = ["#000000"]
    return CoverHashes(
        book_number=book,
        image_path=str(value.get("image_path", "")),
        phash=phash,
        dhash=dhash,
        color_hist=hist_values,
        dominant_colors=dom,
    )


def _load_winner_cover_paths(
    *,
    output_dir: Path,
    catalog_path: Path,
    winner_selections_path: Path,
) -> dict[int, Path]:
    catalog = _load_json_list(catalog_path)
    folder_by_book: dict[int, str] = {}
    for row in catalog:
        book = _safe_int(row.get("number"), 0)
        if book <= 0:
            continue
        folder = str(row.get("folder_name", "")).strip()
        if folder.endswith(" copy"):
            folder = folder[:-5]
        folder_by_book[book] = folder

    selections_payload = _load_json_dict(winner_selections_path)
    selections = selections_payload.get("selections", selections_payload)
    if not isinstance(selections, dict):
        selections = {}

    out: dict[int, Path] = {}
    for key, value in selections.items():
        book = _safe_int(key, 0)
        if book <= 0:
            continue
        variant = _safe_int(value.get("winner") if isinstance(value, dict) else value, 0)
        if variant <= 0:
            continue
        folder = folder_by_book.get(book)
        if not folder:
            continue
        variant_dir = output_dir / folder / f"Variant-{variant}"
        image_path = _find_first_jpg(variant_dir)
        if image_path:
            out[book] = image_path

    if out:
        return out

    # Fallback when selections are missing: pick Variant-1 for each catalog book.
    for book, folder in folder_by_book.items():
        variant_dir = output_dir / folder / "Variant-1"
        image_path = _find_first_jpg(variant_dir)
        if image_path:
            out[book] = image_path
    return out


def _book_title_map(catalog_path: Path) -> dict[int, str]:
    out: dict[int, str] = {}
    for row in _load_json_list(catalog_path):
        book = _safe_int(row.get("number"), 0)
        if book > 0:
            out[book] = str(row.get("title", f"Book {book}"))
    return out


def _load_region_config(path: Path) -> dict[str, Any]:
    payload = _load_json_dict(path)
    if isinstance(payload, dict) and payload.get("consensus_region"):
        return payload
    return {
        "consensus_region": {
            "center_x": 2864,
            "center_y": 1620,
            "radius": 500,
        }
    }


def _load_json_dict(path: Path) -> dict[str, Any]:
    payload = safe_json.load_json(path, {})
    return payload if isinstance(payload, dict) else {}


def _load_json_list(path: Path) -> list[dict[str, Any]]:
    payload = safe_json.load_json(path, [])
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def _find_first_jpg(folder: Path) -> Path | None:
    if not folder.exists():
        return None
    files = sorted(folder.glob("*.jpg"))
    return files[0] if files else None


def _hamming_distance(a: list[int], b: list[int]) -> int:
    if len(a) != len(b):
        n = min(len(a), len(b))
        return sum(int(x != y) for x, y in zip(a[:n], b[:n])) + abs(len(a) - len(b))
    return sum(int(x != y) for x, y in zip(a, b))


def _chi_squared_distance(a: np.ndarray, b: np.ndarray) -> float:
    eps = 1e-10
    num = (a - b) ** 2
    den = a + b + eps
    return float(0.5 * np.sum(num / den))


def _bits_to_hex(bits: np.ndarray) -> str:
    bit_str = "".join(str(int(value)) for value in bits.tolist())
    width = (len(bit_str) + 3) // 4
    return f"{int(bit_str, 2):0{width}x}" if bit_str else ""


def _hex_to_bits(token: str) -> list[int]:
    text = str(token or "").strip().lower().lstrip("0x")
    if not text:
        return []
    bits = bin(int(text, 16))[2:]
    needed = len(text) * 4
    bits = bits.zfill(needed)
    return [1 if ch == "1" else 0 for ch in bits]


def _tokenize_text(text: str) -> list[str]:
    return [token for token in "".join(ch if ch.isalnum() else " " for ch in str(text).lower()).split() if token]


def _relative_or_str(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except Exception:
        return str(path)


def dismiss_similarity_pair(*, book_a: int, book_b: int, dismissed_path: Path | None = None) -> dict[str, Any]:
    if dismissed_path is None:
        runtime = config.get_config()
        dismissed_path = config.similarity_dismissed_path(
            catalog_id=getattr(runtime, "catalog_id", None),
            data_dir=runtime.data_dir,
        )
    payload = _load_json_dict(dismissed_path)
    dismissed = payload.get("pairs", []) if isinstance(payload.get("pairs"), list) else []
    pair_key = f"{min(book_a, book_b)}-{max(book_a, book_b)}"
    if pair_key not in dismissed:
        dismissed.append(pair_key)
    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "pairs": sorted(set(dismissed)),
    }
    safe_json.atomic_write_json(dismissed_path, output)
    return output


def load_dismissed_pairs(dismissed_path: Path | None = None) -> set[str]:
    if dismissed_path is None:
        runtime = config.get_config()
        dismissed_path = config.similarity_dismissed_path(
            catalog_id=getattr(runtime, "catalog_id", None),
            data_dir=runtime.data_dir,
        )
    payload = _load_json_dict(dismissed_path)
    rows = payload.get("pairs", []) if isinstance(payload.get("pairs"), list) else []
    return {str(item) for item in rows}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_books(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    values: set[int] = set()
    for part in str(raw).split(","):
        token = part.strip()
        if not token:
            continue
        if "-" in token:
            a, b = token.split("-", 1)
            lo = _safe_int(a, 0)
            hi = _safe_int(b, 0)
            if lo > 0 and hi > 0:
                for n in range(min(lo, hi), max(lo, hi) + 1):
                    values.add(n)
            continue
        value = _safe_int(token, 0)
        if value > 0:
            values.add(value)
    return sorted(values) if values else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Prompt 11B similarity analysis")
    parser.add_argument("--output-dir", type=Path, default=config.OUTPUT_DIR)
    parser.add_argument("--threshold", type=float, default=0.25)
    parser.add_argument("--book", type=int, default=None, help="Check one book against all winners")
    parser.add_argument("--report", action="store_true", help="Generate HTML similarity report")
    parser.add_argument("--output", type=Path, default=None, help="Report output path for --report")
    parser.add_argument("--catalog", type=str, default=config.DEFAULT_CATALOG_ID, help="Catalog id from config/catalogs.json")
    parser.add_argument("--books", type=str, default=None, help="Reserved for future filtering")
    args = parser.parse_args()

    requested_catalog = str(getattr(args, "catalog", config.DEFAULT_CATALOG_ID) or config.DEFAULT_CATALOG_ID)
    try:
        runtime = config.get_config(requested_catalog)
    except TypeError:  # compatibility for tests/stubs monkeypatching get_config() with no args
        runtime = config.get_config()
    catalog_id = getattr(runtime, "catalog_id", None)
    output_dir = args.output_dir
    _ = _parse_books(args.books)

    summary = run_similarity_analysis(
        output_dir=output_dir,
        threshold=args.threshold,
        catalog_path=runtime.book_catalog_path,
        winner_selections_path=config.winner_selections_path(catalog_id=catalog_id, data_dir=runtime.data_dir),
        regions_path=config.cover_regions_path(catalog_id=catalog_id, config_dir=runtime.config_dir),
        hashes_path=config.similarity_hashes_path(catalog_id=catalog_id, data_dir=runtime.data_dir),
        matrix_path=config.similarity_matrix_path(catalog_id=catalog_id, data_dir=runtime.data_dir),
        clusters_path=config.similarity_clusters_path(catalog_id=catalog_id, data_dir=runtime.data_dir),
    )

    if args.book is not None:
        winners = _load_winner_cover_paths(
            output_dir=output_dir,
            catalog_path=runtime.book_catalog_path,
            winner_selections_path=config.winner_selections_path(catalog_id=catalog_id, data_dir=runtime.data_dir),
        )
        image = winners.get(int(args.book))
        if not image:
            logger.info("Book %s winner image not found.", args.book)
        else:
            check = check_generated_image_against_winners(
                image_path=image,
                book_number=int(args.book),
                output_dir=output_dir,
                catalog_path=runtime.book_catalog_path,
                winner_selections_path=config.winner_selections_path(catalog_id=catalog_id, data_dir=runtime.data_dir),
                regions_path=config.cover_regions_path(catalog_id=catalog_id, config_dir=runtime.config_dir),
                hashes_path=config.similarity_hashes_path(catalog_id=catalog_id, data_dir=runtime.data_dir),
                threshold=args.threshold,
            )
            logger.info("Book %s nearest similarity: %s", args.book, check)

    if args.report:
        report_output = args.output or config.catalog_scoped_data_path(
            "similarity_report.html",
            catalog_id=catalog_id,
            data_dir=runtime.data_dir,
        )
        report_path = generate_report_html(
            matrix_path=config.similarity_matrix_path(catalog_id=catalog_id, data_dir=runtime.data_dir),
            output_path=report_output,
            output_dir=output_dir,
            catalog_path=runtime.book_catalog_path,
            winner_selections_path=config.winner_selections_path(catalog_id=catalog_id, data_dir=runtime.data_dir),
            threshold=args.threshold,
        )
        logger.info("Wrote similarity report: %s", report_path)

    logger.info("Similarity summary: %s", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
