"""LLM-powered intelligent prompt generation and feedback loop (Prompt 11A)."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

try:
    from src import config
    from src import safe_json
    from src.prompt_library import LibraryPrompt, PromptLibrary
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import safe_json  # type: ignore
    from prompt_library import LibraryPrompt, PromptLibrary  # type: ignore
    from logger import get_logger  # type: ignore

logger = get_logger(__name__)

DEFAULT_ENRICHED_CATALOG_PATH = config.CONFIG_DIR / "book_catalog_enriched.json"
DEFAULT_OUTPUT_PATH = config.CONFIG_DIR / "book_prompts_intelligent.json"
DEFAULT_GENRE_PRESETS_PATH = config.CONFIG_DIR / "genre_presets.json"
DEFAULT_PERFORMANCE_PATH = config.prompt_performance_path()
DEFAULT_QUALITY_PATH = config.quality_scores_path()
DEFAULT_HISTORY_PATH = config.generation_history_path()

VARIANT_PLAN = [
    (1, "1_iconic_scene_sketch", "ICONIC SCENE", "sketch"),
    (2, "2_character_portrait_sketch", "CHARACTER PORTRAIT", "sketch"),
    (3, "3_setting_landscape_sketch", "SETTING/LANDSCAPE", "sketch"),
    (4, "4_dramatic_oil_painting", "DRAMATIC OIL PAINTING", "oil"),
    (5, "5_symbolic_allegorical", "SYMBOLIC/ALLEGORICAL", "symbolic"),
]
REQUIRED_COMPOSITION = "circular medallion vignette composition"
REQUIRED_TEXT_BLOCK = "no text, no letters, no words, no watermarks"
REQUIRED_COLOR_BLOCK = "colorful, richly colored"
REQUIRED_SPACE_BLOCK = "no empty space, no plain backgrounds"


@dataclass(slots=True)
class PromptQuality:
    specificity: float
    visual_richness: float
    constraint_compliance: float
    uniqueness: float
    overall: float

    def to_dict(self) -> dict[str, float]:
        return {
            "specificity": round(self.specificity, 4),
            "visual_richness": round(self.visual_richness, 4),
            "constraint_compliance": round(self.constraint_compliance, 4),
            "uniqueness": round(self.uniqueness, 4),
            "overall": round(self.overall, 4),
        }


def generate_prompts(
    *,
    catalog_path: Path = DEFAULT_ENRICHED_CATALOG_PATH,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    books: list[int] | None = None,
    count: int = 5,
    provider: str | None = None,
    model: str | None = None,
    max_tokens: int | None = None,
    genre_presets_path: Path = DEFAULT_GENRE_PRESETS_PATH,
    performance_path: Path | None = None,
    prompt_library_path: Path = config.PROMPT_LIBRARY_PATH,
) -> dict[str, Any]:
    runtime = config.get_config()
    catalog_id = getattr(runtime, "catalog_id", None)
    data_dir = getattr(runtime, "data_dir", None)
    performance_path = performance_path or config.prompt_performance_path(catalog_id=catalog_id, data_dir=data_dir)

    selected_books = set(int(b) for b in (books or []) if int(b) > 0)
    llm_provider = (provider or str(getattr(runtime, "llm_provider", "anthropic")) or "anthropic").strip().lower()
    llm_model = (model or str(getattr(runtime, "llm_model", "gpt-4o")) or "gpt-4o").strip()
    llm_max_tokens = int(max_tokens or int(getattr(runtime, "llm_max_tokens", 2000) or 2000))

    genre_presets = _load_json_dict(genre_presets_path)
    prompt_library = PromptLibrary(prompt_library_path)
    negative_prompt = _default_negative_prompt(runtime.prompt_templates_path)

    source_catalog = _load_json_list(catalog_path)
    existing_output = _load_json_dict(output_path)
    existing_books = {
        _safe_int(book.get("number"), 0): book
        for book in (existing_output.get("books", []) if isinstance(existing_output.get("books"), list) else [])
        if isinstance(book, dict)
    }

    top_patterns = _top_patterns(performance_path)

    compiled: list[dict[str, Any]] = []
    generated_count = 0
    for row in source_catalog:
        if not isinstance(row, dict):
            continue
        number = _safe_int(row.get("number"), 0)
        if number <= 0:
            continue

        if selected_books and number not in selected_books:
            prior = existing_books.get(number)
            if prior:
                compiled.append(prior)
            else:
                compiled.append(_legacy_book_record(row=row, negative_prompt=negative_prompt))
            continue

        generated_count += 1
        prompts = _generate_prompts_for_book(
            row=row,
            count=count,
            provider=llm_provider,
            model=llm_model,
            max_tokens=llm_max_tokens,
            runtime=runtime,
            genre_presets=genre_presets,
            top_patterns=top_patterns,
            negative_prompt=negative_prompt,
        )

        compiled.append(
            {
                "number": number,
                "title": row.get("title", ""),
                "author": row.get("author", ""),
                "folder_name": row.get("folder_name", ""),
                "file_base": row.get("file_base", ""),
                "prompt_source": "intelligent_llm",
                "enrichment": row.get("enrichment", {}),
                "variants": prompts,
            }
        )

    compiled.sort(key=lambda item: _safe_int(item.get("number"), 0))
    payload = {
        "book_count": len(compiled),
        "variant_count_per_book": max(1, min(count, 5)),
        "total_prompts": len(compiled) * max(1, min(count, 5)),
        "prompt_source": "intelligent_llm",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provider": llm_provider,
        "model": llm_model,
        "books": compiled,
    }

    feedback = update_prompt_feedback(
        quality_scores_path=config.quality_scores_path(catalog_id=catalog_id, data_dir=data_dir),
        generation_history_path=config.generation_history_path(catalog_id=catalog_id, data_dir=data_dir),
        prompt_output_path=output_path,
        performance_path=performance_path,
        prompt_library=prompt_library,
        persist=False,
    )
    safe_json.atomic_write_many_json(
        [
            (output_path, payload),
            (performance_path, feedback),
        ]
    )

    summary = {
        "output": str(output_path),
        "books_total": len(compiled),
        "books_generated_in_run": generated_count,
        "feedback": feedback,
    }
    logger.info("Intelligent prompts written: %s books (%s regenerated)", len(compiled), generated_count)
    return summary


def generate_prompts_for_book(
    *,
    book: dict[str, Any],
    provider: str | None = None,
    model: str | None = None,
    max_tokens: int | None = None,
    genre_presets_path: Path = DEFAULT_GENRE_PRESETS_PATH,
    performance_path: Path | None = None,
    runtime: config.Config | None = None,
) -> list[dict[str, Any]]:
    runtime = runtime or config.get_config()
    catalog_id = getattr(runtime, "catalog_id", None)
    data_dir = getattr(runtime, "data_dir", None)
    presets = _load_json_dict(genre_presets_path)
    resolved_performance_path = performance_path or config.prompt_performance_path(catalog_id=catalog_id, data_dir=data_dir)
    patterns = _top_patterns(resolved_performance_path)
    return _generate_prompts_for_book(
        row=book,
        count=5,
        provider=(provider or runtime.llm_provider),
        model=(model or runtime.llm_model),
        max_tokens=(max_tokens or runtime.llm_max_tokens),
        runtime=runtime,
        genre_presets=presets,
        top_patterns=patterns,
        negative_prompt=_default_negative_prompt(runtime.prompt_templates_path),
    )


def _generate_prompts_for_book(
    *,
    row: dict[str, Any],
    count: int,
    provider: str,
    model: str,
    max_tokens: int,
    runtime: config.Config,
    genre_presets: dict[str, Any],
    top_patterns: list[str],
    negative_prompt: str,
) -> list[dict[str, Any]]:
    target_count = max(1, min(int(count), 5))
    base_prompts = _llm_generate_variant_prompts(
        row=row,
        provider=provider,
        model=model,
        max_tokens=max_tokens,
        runtime=runtime,
        genre_presets=genre_presets,
        top_patterns=top_patterns,
    )
    if len(base_prompts) < target_count:
        base_prompts = _fallback_variant_prompts(row)

    base_prompts = base_prompts[:target_count]

    variants: list[dict[str, Any]] = []
    prompt_texts: list[str] = []
    for idx, (variant_id, variant_key, variant_name, _style) in enumerate(VARIANT_PLAN[:target_count]):
        candidate = base_prompts[idx] if idx < len(base_prompts) else ""
        if not candidate.strip():
            candidate = _fallback_variant_prompts(row)[idx]

        final_prompt, quality = _enforce_quality_loop(
            prompt=candidate,
            row=row,
            variant_name=variant_name,
            provider=provider,
            model=model,
            max_tokens=max_tokens,
            runtime=runtime,
            peer_prompts=prompt_texts,
            genre_presets=genre_presets,
            top_patterns=top_patterns,
        )
        prompt_texts.append(final_prompt)

        variants.append(
            {
                "variant_id": variant_id,
                "variant_key": variant_key,
                "variant_name": variant_name,
                "description": _variant_description(row=row, index=idx),
                "prompt": final_prompt,
                "negative_prompt": negative_prompt,
                "style_reference": "intelligent_llm",
                "word_count": _word_count(final_prompt),
                "quality": quality.to_dict(),
            }
        )

    return variants


def _enforce_quality_loop(
    *,
    prompt: str,
    row: dict[str, Any],
    variant_name: str,
    provider: str,
    model: str,
    max_tokens: int,
    runtime: config.Config,
    peer_prompts: list[str],
    genre_presets: dict[str, Any],
    top_patterns: list[str],
) -> tuple[str, PromptQuality]:
    current = _ensure_prompt_constraints(prompt)
    quality = _score_prompt(current, row=row, peers=peer_prompts)

    if quality.overall >= 0.7:
        return current, quality

    for attempt in range(1, 4):
        regenerated = _llm_regenerate_single(
            row=row,
            current=current,
            quality=quality,
            variant_name=variant_name,
            provider=provider,
            model=model,
            max_tokens=max_tokens,
            runtime=runtime,
            genre_presets=genre_presets,
            top_patterns=top_patterns,
            attempt=attempt,
        )
        if not regenerated:
            regenerated = _fallback_variant_prompts(row)[min(len(VARIANT_PLAN) - 1, max(0, len(peer_prompts)))]

        current = _ensure_prompt_constraints(regenerated)
        quality = _score_prompt(current, row=row, peers=peer_prompts)
        if quality.overall >= 0.7:
            return current, quality

    return current, quality


def _llm_generate_variant_prompts(
    *,
    row: dict[str, Any],
    provider: str,
    model: str,
    max_tokens: int,
    runtime: config.Config,
    genre_presets: dict[str, Any],
    top_patterns: list[str],
) -> list[str]:
    fallback = _fallback_variant_prompts(row)
    api_key = _provider_api_key(provider=provider, runtime=runtime)
    if not api_key:
        return fallback

    enrichment = row.get("enrichment", {}) if isinstance(row.get("enrichment"), dict) else {}
    genre_key = _genre_key(str(enrichment.get("genre", "")))
    genre_preset = genre_presets.get(genre_key, {}) if isinstance(genre_presets, dict) else {}

    user_payload = {
        "book_number": _safe_int(row.get("number"), 0),
        "title": row.get("title", ""),
        "author": row.get("author", ""),
        "enrichment": enrichment,
        "genre_preset": genre_preset,
        "top_patterns": top_patterns,
        "instruction": "Return strict JSON with key 'prompts' as an array of exactly 5 prompts in order: iconic scene, character portrait, setting/landscape, dramatic oil painting, symbolic/allegorical.",
    }

    response_text = _call_llm_json(
        provider=provider,
        api_key=api_key,
        model=model,
        max_tokens=max_tokens,
        system_prompt=_intelligent_system_prompt(),
        user_prompt=json.dumps(user_payload, ensure_ascii=False),
    )
    parsed = _parse_json(response_text)
    prompts = parsed.get("prompts", []) if isinstance(parsed, dict) else []

    extracted: list[str] = []
    if isinstance(prompts, list):
        for item in prompts:
            if isinstance(item, str):
                extracted.append(item)
            elif isinstance(item, dict):
                extracted.append(str(item.get("prompt", "")))
            if len(extracted) >= 5:
                break

    if len(extracted) < 5:
        return fallback
    return [_ensure_prompt_constraints(text) for text in extracted[:5]]


def _llm_regenerate_single(
    *,
    row: dict[str, Any],
    current: str,
    quality: PromptQuality,
    variant_name: str,
    provider: str,
    model: str,
    max_tokens: int,
    runtime: config.Config,
    genre_presets: dict[str, Any],
    top_patterns: list[str],
    attempt: int,
) -> str:
    api_key = _provider_api_key(provider=provider, runtime=runtime)
    if not api_key:
        return ""

    enrichment = row.get("enrichment", {}) if isinstance(row.get("enrichment"), dict) else {}
    genre_key = _genre_key(str(enrichment.get("genre", "")))
    genre_preset = genre_presets.get(genre_key, {}) if isinstance(genre_presets, dict) else {}

    user_payload = {
        "book_number": _safe_int(row.get("number"), 0),
        "title": row.get("title", ""),
        "author": row.get("author", ""),
        "variant": variant_name,
        "attempt": attempt,
        "current_prompt": current,
        "quality": quality.to_dict(),
        "enrichment": enrichment,
        "genre_preset": genre_preset,
        "top_patterns": top_patterns,
        "instruction": "Rewrite this single prompt to improve low scoring dimensions while preserving strict constraints.",
    }

    response_text = _call_llm_json(
        provider=provider,
        api_key=api_key,
        model=model,
        max_tokens=max_tokens,
        system_prompt=_intelligent_regen_system_prompt(),
        user_prompt=json.dumps(user_payload, ensure_ascii=False),
    )
    parsed = _parse_json(response_text)
    if isinstance(parsed, dict):
        if isinstance(parsed.get("prompt"), str):
            return str(parsed["prompt"])
        if isinstance(parsed.get("rewritten_prompt"), str):
            return str(parsed["rewritten_prompt"])
    return ""


def _call_llm_json(
    *,
    provider: str,
    api_key: str,
    model: str,
    max_tokens: int,
    system_prompt: str,
    user_prompt: str,
) -> str:
    provider = provider.strip().lower()

    if provider == "anthropic":
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": max_tokens,
                "temperature": 0.35,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
            },
            timeout=90,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Anthropic error {response.status_code}: {response.text[:240]}")
        body = response.json()
        text_parts: list[str] = []
        for part in body.get("content", []):
            if isinstance(part, dict) and str(part.get("type")) == "text":
                text_parts.append(str(part.get("text", "")))
        return "\n".join(text_parts)

    if provider == "openai":
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": max_tokens,
                "temperature": 0.35,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            },
            timeout=90,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"OpenAI error {response.status_code}: {response.text[:240]}")
        body = response.json()
        choices = body.get("choices", [])
        if choices:
            message = choices[0].get("message", {})
            return str(message.get("content", "") or "")
        return ""

    raise RuntimeError(f"Unsupported provider: {provider}")


def update_prompt_feedback(
    *,
    quality_scores_path: Path = DEFAULT_QUALITY_PATH,
    generation_history_path: Path = DEFAULT_HISTORY_PATH,
    prompt_output_path: Path = DEFAULT_OUTPUT_PATH,
    performance_path: Path = DEFAULT_PERFORMANCE_PATH,
    prompt_library: PromptLibrary | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    prompt_library = prompt_library or PromptLibrary(config.PROMPT_LIBRARY_PATH)

    quality_payload = _load_json_dict(quality_scores_path)
    quality_rows = quality_payload.get("scores", []) if isinstance(quality_payload.get("scores"), list) else []
    by_key: dict[tuple[int, int], float] = {}
    for row in quality_rows:
        if not isinstance(row, dict):
            continue
        key = (
            _safe_int(row.get("book_number"), 0),
            _safe_int(row.get("variant_id", row.get("variant")), 0),
        )
        score = _safe_float(row.get("overall_score"), 0.0)
        by_key[key] = max(by_key.get(key, 0.0), score)

    history_payload = _load_json_dict(generation_history_path)
    history_rows = history_payload.get("items", []) if isinstance(history_payload.get("items"), list) else []

    pattern_stats: dict[str, dict[str, float]] = {
        "specific_character_action": {"sum": 0.0, "count": 0},
        "generic_scene_description": {"sum": 0.0, "count": 0},
        "symbolic_with_color_direction": {"sum": 0.0, "count": 0},
    }

    auto_saved = 0
    for row in history_rows:
        if not isinstance(row, dict):
            continue
        prompt = str(row.get("prompt", "")).strip()
        if not prompt:
            continue

        book = _safe_int(row.get("book_number"), 0)
        variant = _safe_int(row.get("variant", row.get("variant_id")), 0)
        quality = by_key.get((book, variant), _safe_float(row.get("quality_score"), 0.0))
        if quality <= 0:
            continue

        pattern = _classify_pattern(prompt)
        bucket = pattern_stats.setdefault(pattern, {"sum": 0.0, "count": 0})
        bucket["sum"] += quality
        bucket["count"] += 1

        if quality >= 0.85:
            template = _templateize_prompt(prompt, title_hint=str(row.get("book_title", "")).strip())
            if "{title}" not in template:
                continue
            library_prompt = LibraryPrompt(
                id=f"intelligent-{book}-{variant}-{abs(hash(template)) % 1_000_000}",
                name=f"Intelligent High Score {book}-{variant}",
                prompt_template=template,
                style_anchors=["allegorical_symbolic"],
                negative_prompt="text, letters, words, watermark, signature",
                source_book=str(book),
                source_model=str(row.get("model", "unknown")),
                quality_score=round(quality, 4),
                saved_by="auto-intelligent",
                created_at=datetime.now(timezone.utc).isoformat(),
                notes="Auto-saved from high quality intelligent prompt run",
                tags=["intelligent", "auto-high-score"],
            )
            try:
                prompt_library.save_prompt(library_prompt)
                auto_saved += 1
            except Exception:
                # Ignore duplicate/invalid templates.
                pass

    patterns_out: dict[str, dict[str, float]] = {}
    for key, values in pattern_stats.items():
        count = int(values.get("count", 0) or 0)
        avg = (float(values.get("sum", 0.0)) / count) if count else 0.0
        patterns_out[key] = {
            "avg_score": round(avg, 4),
            "count": count,
        }

    perf_payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "patterns": patterns_out,
        "auto_saved_prompts": auto_saved,
        "source_prompt_file": str(prompt_output_path),
    }
    if persist:
        safe_json.atomic_write_json(performance_path, perf_payload)
    return perf_payload


def _classify_pattern(prompt: str) -> str:
    text = prompt.lower()
    has_symbolic = any(token in text for token in ["symbolic", "allegorical", "metaphor", "emblem"])
    has_color = any(token in text for token in ["sepia", "gold", "amber", "palette", "chiaroscuro"])
    has_character = any(token in text for token in ["captain", "lady", "king", "lucy", "ahab", "portrait"])

    if has_symbolic and has_color:
        return "symbolic_with_color_direction"
    if has_character:
        return "specific_character_action"
    return "generic_scene_description"


def _templateize_prompt(prompt: str, title_hint: str = "") -> str:
    text = str(prompt).strip()
    if not text:
        return ""
    if "{title}" in text:
        return text

    if title_hint:
        pattern = re.compile(re.escape(title_hint), flags=re.IGNORECASE)
        text, count = pattern.subn("{title}", text)
        if count > 0:
            return text

    return f"{text}, inspired by {{title}}"


def _variant_description(*, row: dict[str, Any], index: int) -> str:
    enrichment = row.get("enrichment", {}) if isinstance(row.get("enrichment"), dict) else {}
    scenes = enrichment.get("iconic_scenes", []) if isinstance(enrichment.get("iconic_scenes"), list) else []
    motifs = enrichment.get("visual_motifs", []) if isinstance(enrichment.get("visual_motifs"), list) else []
    if index < len(scenes):
        return str(scenes[index])
    if motifs:
        return str(motifs[index % len(motifs)])
    return f"Book-specific intelligent variant {index + 1}"


def _fallback_variant_prompts(row: dict[str, Any]) -> list[str]:
    enrichment = row.get("enrichment", {}) if isinstance(row.get("enrichment"), dict) else {}
    scenes = [str(item) for item in enrichment.get("iconic_scenes", []) if str(item).strip()]
    motifs = [str(item) for item in enrichment.get("visual_motifs", []) if str(item).strip()]
    protagonist = str(enrichment.get("protagonist", "central protagonist") or "central protagonist")
    setting = str(enrichment.get("setting_primary", "period setting") or "period setting")
    theme = str(enrichment.get("emotional_tone", "dramatic emotional arc") or "dramatic emotional arc")

    scene_1 = scenes[0] if scenes else f"Iconic scene from {row.get('title', 'the story')}"
    scene_2 = scenes[1] if len(scenes) > 1 else f"Defining portrait moment of {protagonist}"
    scene_3 = scenes[2] if len(scenes) > 2 else f"Landscape view of {setting}"
    motif = motifs[0] if motifs else "symbolic visual motif"

    prompts = [
        f"Colorful richly colored historical scene of {scene_1}, with deep crimson, burnt sienna, imperial gold, slate blue-grey, and amber light, dense detail and strong focal storytelling, {REQUIRED_COMPOSITION}, {REQUIRED_TEXT_BLOCK}",
        f"Colorful richly colored engraved portrait of {protagonist} in a defining narrative instant, with ruby, emerald, sapphire, ivory, and bronze accents, expressive posture and layered atmosphere, {REQUIRED_COMPOSITION}, {REQUIRED_TEXT_BLOCK}",
        f"Colorful richly colored setting-focused scene of {scene_3}, with terracotta, fresco blue, ochre, umber, and birch-white tones, architectural depth and edge-to-edge detail, {REQUIRED_COMPOSITION}, {REQUIRED_TEXT_BLOCK}",
        f"Colorful richly colored dramatic oil painting of a pivotal moment tied to {theme}, with molten gold, indigo, blood orange, silver-white, and ocean teal contrast, dynamic motion and dense composition, {REQUIRED_COMPOSITION}, {REQUIRED_TEXT_BLOCK}",
        f"Colorful richly colored symbolic allegorical illustration using {motif} to represent core themes, with deep purple, amethyst, absinthe green, lapis blue, and antique gold, layered iconography and full-frame detail, {REQUIRED_COMPOSITION}, {REQUIRED_TEXT_BLOCK}",
    ]
    return [_ensure_prompt_constraints(text) for text in prompts]


def _score_prompt(prompt: str, *, row: dict[str, Any], peers: list[str]) -> PromptQuality:
    text = str(prompt).strip()
    words = _word_count(text)
    lower = text.lower()

    enrichment = row.get("enrichment", {}) if isinstance(row.get("enrichment"), dict) else {}
    key_tokens: set[str] = set()
    for key in ["protagonist", "setting_primary", "emotional_tone", "genre", "era"]:
        key_tokens.update(_tokenize(str(enrichment.get(key, ""))))
    for key in ["key_characters", "iconic_scenes", "visual_motifs", "symbolic_elements"]:
        values = enrichment.get(key, [])
        if isinstance(values, list):
            for item in values:
                key_tokens.update(_tokenize(str(item)))

    key_tokens = {tok for tok in key_tokens if len(tok) >= 4}
    prompt_tokens = set(_tokenize(lower))
    overlap = len(prompt_tokens.intersection(key_tokens))
    specificity = _clip(overlap / 8.0)

    visual_terms = {
        "light",
        "shadow",
        "sepia",
        "gold",
        "amber",
        "parchment",
        "brushwork",
        "crosshatching",
        "chiaroscuro",
        "palette",
        "texture",
        "composition",
        "atmospheric",
        "foreground",
        "background",
    }
    visual_hits = sum(1 for term in visual_terms if term in lower)
    visual_richness = _clip(visual_hits / 8.0)

    has_required = (
        REQUIRED_COMPOSITION in lower
        and REQUIRED_TEXT_BLOCK in lower
        and REQUIRED_COLOR_BLOCK in lower
        and REQUIRED_SPACE_BLOCK in lower
    )
    word_ok = 40 <= words <= 80
    compliance = 1.0
    if not has_required:
        compliance -= 0.45
    if not word_ok:
        compliance -= 0.35
    compliance = _clip(compliance)

    uniqueness = 1.0
    if peers:
        sims = [_token_jaccard(lower, other.lower()) for other in peers if other]
        if sims:
            uniqueness = _clip(1.0 - max(sims))

    overall = (specificity + visual_richness + compliance + uniqueness) / 4.0
    return PromptQuality(
        specificity=specificity,
        visual_richness=visual_richness,
        constraint_compliance=compliance,
        uniqueness=uniqueness,
        overall=overall,
    )


def _ensure_prompt_constraints(prompt: str) -> str:
    text = re.sub(r"\s+", " ", str(prompt or "")).strip().strip(",")
    if not text:
        text = "Classical illustration with strong narrative detail"

    lower = text.lower()
    if REQUIRED_COMPOSITION not in lower:
        text += f", {REQUIRED_COMPOSITION}"
        lower = text.lower()
    if REQUIRED_TEXT_BLOCK not in lower:
        text += f", {REQUIRED_TEXT_BLOCK}"
        lower = text.lower()
    if REQUIRED_COLOR_BLOCK not in lower:
        text += f", {REQUIRED_COLOR_BLOCK}"
        lower = text.lower()
    if REQUIRED_SPACE_BLOCK not in lower:
        text += f", {REQUIRED_SPACE_BLOCK}"

    filler = "warm lighting contrast, refined period detail, coherent focal subject, no empty space"
    while _word_count(text) < 40:
        text += ", " + filler

    if _word_count(text) > 80:
        text = " ".join(text.split()[:80]).rstrip(",")
    return text


def _legacy_book_record(*, row: dict[str, Any], negative_prompt: str) -> dict[str, Any]:
    variants: list[dict[str, Any]] = []
    for idx, (variant_id, variant_key, variant_name, _) in enumerate(VARIANT_PLAN):
        prompt = _fallback_variant_prompts(row)[idx]
        quality = _score_prompt(prompt, row=row, peers=[v["prompt"] for v in variants])
        variants.append(
            {
                "variant_id": variant_id,
                "variant_key": variant_key,
                "variant_name": variant_name,
                "description": _variant_description(row=row, index=idx),
                "prompt": prompt,
                "negative_prompt": negative_prompt,
                "style_reference": "fallback",
                "word_count": _word_count(prompt),
                "quality": quality.to_dict(),
            }
        )
    return {
        "number": _safe_int(row.get("number"), 0),
        "title": row.get("title", ""),
        "author": row.get("author", ""),
        "folder_name": row.get("folder_name", ""),
        "file_base": row.get("file_base", ""),
        "prompt_source": "fallback",
        "enrichment": row.get("enrichment", {}),
        "variants": variants,
    }


def _default_negative_prompt(prompt_templates_path: Path) -> str:
    payload = _load_json_dict(prompt_templates_path)
    text = str(payload.get("negative_prompt", "")).strip()
    return text or "text, letters, words, watermark, signature, photorealistic, cartoon"


def _provider_api_key(*, provider: str, runtime: config.Config) -> str:
    provider_key_map = {
        "anthropic": str(getattr(runtime, "anthropic_api_key", "") or ""),
        "openai": str(getattr(runtime, "openai_api_key", "") or ""),
    }
    return provider_key_map.get(provider.strip().lower(), "").strip()


def _genre_key(raw_genre: str) -> str:
    text = raw_genre.lower().strip()
    if not text:
        return "literary_fiction"
    mapping = {
        "literary": "literary_fiction",
        "gothic": "gothic_horror",
        "horror": "gothic_horror",
        "adventure": "adventure",
        "science": "science_fiction",
        "fiction": "science_fiction",
        "romance": "romance",
        "philosophy": "philosophy",
        "satire": "satire",
        "comedy": "satire",
    }
    for key, value in mapping.items():
        if key in text:
            return value
    return "literary_fiction"


def _intelligent_system_prompt() -> str:
    return (
        "You are an art director for classical book cover illustrations. "
        "You write prompts for AI image generators (FLUX, GPT Image, Imagen) that produce illustrations for circular medallion frames on book covers. "
        "CONSTRAINTS: output must work as a circular medallion vignette composition for a luxury leather-bound edition; style must be classical (oil painting, pen-and-ink sketch, engraving) and never photorealistic/cartoonish; "
        "must include phrase 'no text, no letters, no words, no watermarks'; must include phrase 'colorful, richly colored'; must include phrase 'no empty space, no plain backgrounds'; each prompt 40-80 words; highly book-specific and recognizable. "
        "Return JSON only."
    )


def _intelligent_regen_system_prompt() -> str:
    return (
        "You rewrite one classical illustration prompt to increase specificity, visual richness, uniqueness, and constraint compliance. "
        "Return JSON with key 'prompt'. Must include required phrases and 40-80 words."
    )


def _top_patterns(path: Path) -> list[str]:
    payload = _load_json_dict(path)
    patterns = payload.get("patterns", {}) if isinstance(payload.get("patterns"), dict) else {}
    ranked: list[tuple[str, float]] = []
    for key, values in patterns.items():
        if not isinstance(values, dict):
            continue
        score = _safe_float(values.get("avg_score"), 0.0)
        ranked.append((str(key), score))
    ranked.sort(key=lambda item: item[1], reverse=True)
    return [name for name, _score in ranked[:3]]


def _word_count(text: str) -> int:
    return len(str(text or "").split())


def _tokenize(text: str) -> list[str]:
    return [token for token in re.split(r"[^a-z0-9']+", text.lower()) if token]


def _token_jaccard(a: str, b: str) -> float:
    sa = set(_tokenize(a))
    sb = set(_tokenize(b))
    if not sa and not sb:
        return 0.0
    inter = len(sa.intersection(sb))
    union = len(sa.union(sb))
    return inter / max(1, union)


def _clip(value: float) -> float:
    return float(max(0.0, min(1.0, value)))


def _parse_books(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    values: set[int] = set()
    for token in str(raw).split(","):
        part = token.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            lo = _safe_int(a, 0)
            hi = _safe_int(b, 0)
            if lo > 0 and hi > 0:
                for n in range(min(lo, hi), max(lo, hi) + 1):
                    values.add(n)
            continue
        value = _safe_int(part, 0)
        if value > 0:
            values.add(value)
    return sorted(values) if values else None


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


def _parse_json(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return {}
    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def _load_json_dict(path: Path) -> dict[str, Any]:
    payload = safe_json.load_json(path, {})
    return payload if isinstance(payload, dict) else {}


def _load_json_list(path: Path) -> list[dict[str, Any]]:
    payload = safe_json.load_json(path, [])
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="Prompt 11A: intelligent prompt generation")
    parser.add_argument("--catalog", type=Path, default=DEFAULT_ENRICHED_CATALOG_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--books", type=str, default=None)
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--provider", type=str, default=None)
    parser.add_argument("--model", type=str, default=None)
    parser.add_argument("--max-tokens", type=int, default=None)
    parser.add_argument("--genre-presets", type=Path, default=DEFAULT_GENRE_PRESETS_PATH)
    parser.add_argument("--performance", type=Path, default=DEFAULT_PERFORMANCE_PATH)
    args = parser.parse_args()

    summary = generate_prompts(
        catalog_path=args.catalog,
        output_path=args.output,
        books=_parse_books(args.books),
        count=args.count,
        provider=args.provider,
        model=args.model,
        max_tokens=args.max_tokens,
        genre_presets_path=args.genre_presets,
        performance_path=args.performance,
    )
    logger.info("Intelligent prompt summary: %s", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
