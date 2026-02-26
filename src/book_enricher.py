"""LLM-powered catalog enrichment for Alexandria cover prompts (Prompt 11A)."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

try:
    from src import config
    from src import safe_json
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import safe_json  # type: ignore
    from logger import get_logger  # type: ignore

logger = get_logger(__name__)

DEFAULT_OUTPUT_PATH = config.CONFIG_DIR / "book_catalog_enriched.json"
DEFAULT_USAGE_PATH = config.llm_usage_path()
DEFAULT_DESCRIPTIONS_PATH = config.CONFIG_DIR / "book_descriptions.json"


@dataclass(slots=True)
class UsageCounters:
    total_calls: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cost_usd: float = 0.0

    def add(self, input_tokens: int, output_tokens: int, cost_per_1k: float) -> None:
        self.total_calls += 1
        self.total_input_tokens += max(0, int(input_tokens))
        self.total_output_tokens += max(0, int(output_tokens))
        self.total_cost_usd += ((max(0, int(input_tokens)) + max(0, int(output_tokens))) / 1000.0) * float(cost_per_1k)


def enrich_catalog(
    *,
    catalog_path: Path,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    books: list[int] | None = None,
    force_refresh: bool = False,
    provider: str | None = None,
    model: str | None = None,
    max_tokens: int | None = None,
    cost_per_1k_tokens: float | None = None,
    usage_path: Path = DEFAULT_USAGE_PATH,
    descriptions_path: Path = DEFAULT_DESCRIPTIONS_PATH,
) -> dict[str, Any]:
    """Enrich catalog entries with genre/scenes/motifs metadata."""
    runtime = config.get_config()

    llm_provider = (provider or str(getattr(runtime, "llm_provider", "anthropic")) or "anthropic").strip().lower()
    llm_model = (model or str(getattr(runtime, "llm_model", "gpt-4o")) or "gpt-4o").strip()
    llm_max_tokens = int(max_tokens or int(getattr(runtime, "llm_max_tokens", 2000) or 2000))
    llm_cost = float(cost_per_1k_tokens or float(getattr(runtime, "llm_cost_per_1k_tokens", 0.003) or 0.003))

    source_catalog = _load_json_list(catalog_path)
    existing_catalog = _load_json_list(output_path)
    existing_by_number = {
        _safe_int(item.get("number"), 0): item
        for item in existing_catalog
        if isinstance(item, dict) and _safe_int(item.get("number"), 0) > 0
    }

    requested = set(int(b) for b in (books or []) if int(b) > 0)
    descriptions = _load_descriptions(descriptions_path)

    usage = UsageCounters()
    output_rows: list[dict[str, Any]] = []
    enriched_count = 0

    for row in source_catalog:
        if not isinstance(row, dict):
            continue

        number = _safe_int(row.get("number"), 0)
        if number <= 0:
            continue

        existing_row = existing_by_number.get(number, {})
        existing_enrichment = existing_row.get("enrichment") if isinstance(existing_row, dict) else None
        target_row = dict(existing_row) if isinstance(existing_row, dict) else dict(row)

        should_attempt = False
        if requested:
            should_attempt = number in requested
        elif force_refresh:
            should_attempt = True
        else:
            should_attempt = not isinstance(existing_enrichment, dict) or not existing_enrichment

        if should_attempt:
            enrichment, in_tok, out_tok, source = _generate_enrichment(
                row=row,
                description=descriptions.get(str(number), ""),
                provider=llm_provider,
                model=llm_model,
                max_tokens=llm_max_tokens,
                runtime=runtime,
            )
            if source == "llm":
                usage.add(in_tok, out_tok, llm_cost)
            target_row = dict(row)
            target_row["enrichment"] = _normalize_enrichment(enrichment, row)
            enriched_count += 1
        else:
            # Keep previously enriched data if available.
            target_row = dict(row)
            if isinstance(existing_enrichment, dict):
                target_row["enrichment"] = _normalize_enrichment(existing_enrichment, row)

        output_rows.append(target_row)

    usage_summary = _merge_usage(
        usage_path=usage_path,
        run_usage=usage,
        enriched_count=enriched_count,
        provider=llm_provider,
        model=llm_model,
    )
    safe_json.atomic_write_many_json(
        [
            (output_path, output_rows),
            (usage_path, usage_summary),
        ]
    )

    summary = {
        "catalog": str(catalog_path),
        "output": str(output_path),
        "books_total": len(output_rows),
        "books_enriched_in_run": enriched_count,
        "provider": llm_provider,
        "model": llm_model,
        "usage": usage_summary,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info(
        "Enrichment complete: %s books written to %s (%s enriched in this run)",
        len(output_rows),
        output_path,
        enriched_count,
    )
    return summary


def _generate_enrichment(
    *,
    row: dict[str, Any],
    description: str,
    provider: str,
    model: str,
    max_tokens: int,
    runtime: config.Config,
) -> tuple[dict[str, Any], int, int, str]:
    # Fallback always available for offline/test mode.
    fallback = _fallback_enrichment(row=row, description=description)

    if provider == "anthropic":
        api_key = str(getattr(runtime, "anthropic_api_key", "") or "").strip()
        if not api_key:
            return fallback, 0, 0, "fallback"
        try:
            payload = _call_anthropic(
                api_key=api_key,
                model=model,
                max_tokens=max_tokens,
                row=row,
                description=description,
            )
            return payload["enrichment"], payload["input_tokens"], payload["output_tokens"], "llm"
        except Exception as exc:
            logger.warning("Anthropic enrichment failed for book %s: %s", row.get("number"), exc)
            return fallback, 0, 0, "fallback"

    if provider == "openai":
        api_key = str(getattr(runtime, "openai_api_key", "") or "").strip()
        if not api_key:
            return fallback, 0, 0, "fallback"
        try:
            payload = _call_openai(
                api_key=api_key,
                model=model,
                max_tokens=max_tokens,
                row=row,
                description=description,
            )
            return payload["enrichment"], payload["input_tokens"], payload["output_tokens"], "llm"
        except Exception as exc:
            logger.warning("OpenAI enrichment failed for book %s: %s", row.get("number"), exc)
            return fallback, 0, 0, "fallback"

    logger.warning("Unsupported LLM provider '%s'; using fallback enrichment", provider)
    return fallback, 0, 0, "fallback"


def _call_anthropic(
    *,
    api_key: str,
    model: str,
    max_tokens: int,
    row: dict[str, Any],
    description: str,
) -> dict[str, Any]:
    user_prompt = _build_enrichment_prompt(row=row, description=description)
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
            "temperature": 0.3,
            "system": _enrichment_system_prompt(),
            "messages": [{"role": "user", "content": user_prompt}],
        },
        timeout=90,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Anthropic error {response.status_code}: {response.text[:240]}")

    body = response.json()
    content = body.get("content", [])
    text_parts: list[str] = []
    for part in content:
        if isinstance(part, dict) and str(part.get("type")) == "text":
            text_parts.append(str(part.get("text", "")))
    raw = "\n".join(text_parts).strip()
    parsed = _parse_json_object(raw)

    usage = body.get("usage", {}) if isinstance(body, dict) else {}
    in_tok = _safe_int(usage.get("input_tokens"), 0)
    out_tok = _safe_int(usage.get("output_tokens"), 0)

    return {
        "enrichment": parsed,
        "input_tokens": in_tok,
        "output_tokens": out_tok,
    }


def _call_openai(
    *,
    api_key: str,
    model: str,
    max_tokens: int,
    row: dict[str, Any],
    description: str,
) -> dict[str, Any]:
    user_prompt = _build_enrichment_prompt(row=row, description=description)
    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "temperature": 0.3,
            "max_tokens": max_tokens,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": _enrichment_system_prompt()},
                {"role": "user", "content": user_prompt},
            ],
        },
        timeout=90,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"OpenAI error {response.status_code}: {response.text[:240]}")

    body = response.json()
    choices = body.get("choices", [])
    message = choices[0].get("message", {}) if choices else {}
    raw = str(message.get("content", "") or "")
    parsed = _parse_json_object(raw)

    usage = body.get("usage", {}) if isinstance(body, dict) else {}
    in_tok = _safe_int(usage.get("prompt_tokens"), 0)
    out_tok = _safe_int(usage.get("completion_tokens"), 0)

    return {
        "enrichment": parsed,
        "input_tokens": in_tok,
        "output_tokens": out_tok,
    }


def _build_enrichment_prompt(*, row: dict[str, Any], description: str) -> str:
    number = _safe_int(row.get("number"), 0)
    title = str(row.get("title", "")).strip()
    author = str(row.get("author", "")).strip()
    base = [
        f"Book number: {number}",
        f"Title: {title}",
        f"Author: {author}",
    ]
    if description.strip():
        base.append(f"Description: {description.strip()}")

    base.append(
        "Return strict JSON with keys: "
        "genre, era, setting_primary, setting_details, protagonist, key_characters, iconic_scenes, "
        "visual_motifs, emotional_tone, color_palette_suggestion, art_period_match, symbolic_elements."
    )
    base.append("Use concise but specific text for classical literature artwork planning.")
    return "\n".join(base)


def _enrichment_system_prompt() -> str:
    return (
        "You are a literary art director. Produce concise, visual, historically grounded metadata for classic books. "
        "Output valid JSON only, no markdown. Prefer concrete characters/scenes/settings over generic phrasing."
    )


def _fallback_enrichment(*, row: dict[str, Any], description: str) -> dict[str, Any]:
    title = str(row.get("title", "")).strip()
    author = str(row.get("author", "")).strip()
    title_lower = title.lower()
    genre = _guess_genre(title_lower=title_lower, author=author)
    setting = _guess_setting(title_lower=title_lower)

    protagonist = "Central protagonist"
    if "moby dick" in title_lower or "whale" in title_lower:
        protagonist = "Captain Ahab"
    elif "dracula" in title_lower:
        protagonist = "Count Dracula"
    elif "alice" in title_lower:
        protagonist = "Alice"

    scene_a = f"Iconic turning point from {title}"
    scene_b = f"Defining confrontation involving {protagonist}"
    scene_c = f"Atmospheric setting moment that signals the themes of {title}"
    motif_a = "Period costume and historically grounded props"
    motif_b = "Symbolic object tied to the story's moral tension"
    motif_c = "Dramatic light and weather matching emotional stakes"

    if description.strip():
        nouns = re.findall(r"[A-Za-z][A-Za-z'\-]{3,}", description)
        if nouns:
            token = nouns[0]
            scene_a = f"A pivotal moment centered on {token}"
            motif_b = f"Symbolic focus on {token} as thematic anchor"

    return {
        "genre": genre,
        "era": _guess_era(title_lower=title_lower),
        "setting_primary": setting,
        "setting_details": f"Narrative spaces associated with {title}",
        "protagonist": protagonist,
        "key_characters": [protagonist, "Supporting cast", "Antagonistic force", "Mentor/foil"],
        "iconic_scenes": [scene_a, scene_b, scene_c],
        "visual_motifs": [motif_a, motif_b, motif_c, "Circular medallion-ready composition"],
        "emotional_tone": "Classical dramatic tension, narrative consequence, and literary depth",
        "color_palette_suggestion": "Warm sepia and gold accents with restrained cool shadows",
        "art_period_match": "19th-century engraving / classical oil illustration",
        "symbolic_elements": [
            "A recurring object representing fate",
            "Light versus shadow for moral conflict",
            "Spatial contrast between confinement and freedom",
        ],
    }


def _guess_genre(*, title_lower: str, author: str) -> str:
    author_lower = author.lower()
    if any(token in title_lower for token in ["dracula", "frankenstein", "jungle", "island", "whale"]):
        return "Adventure / Gothic Classic"
    if any(token in title_lower for token in ["pride", "prejudice", "room with a view", "jane", "sense"]):
        return "Literary Fiction / Social Novel"
    if any(token in title_lower for token in ["hamlet", "romeo", "oedipus"]):
        return "Classical Tragedy"
    if any(token in author_lower for token in ["dostoev", "kafka", "camus"]):
        return "Psychological / Philosophical Fiction"
    if any(token in title_lower for token in ["time", "invisible", "twenty thousand", "journey"]):
        return "Speculative / Science Fiction Classic"
    return "Classic Literary Fiction"


def _guess_setting(*, title_lower: str) -> str:
    if any(token in title_lower for token in ["moby", "whale", "sea", "ocean"]):
        return "Maritime world of ships and stormy seas"
    if any(token in title_lower for token in ["dracula", "castle", "gothic"]):
        return "Castle interiors and moonlit European landscapes"
    if any(token in title_lower for token in ["pride", "prejudice", "room", "view"]):
        return "English estates and European travel settings"
    if any(token in title_lower for token in ["jungle", "island", "wild"]):
        return "Wilderness landscapes and frontier environments"
    return "Period-appropriate settings central to the narrative"


def _guess_era(*, title_lower: str) -> str:
    if any(token in title_lower for token in ["hamlet", "romeo", "oedipus"]):
        return "Classical / Renaissance-era dramatic tradition"
    if any(token in title_lower for token in ["moby", "whale", "dickens", "victorian", "dracula"]):
        return "19th-century literary era"
    return "Historically grounded era aligned to original publication context"


def _normalize_enrichment(enrichment: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    fallback = _fallback_enrichment(row=row, description="")
    merged = dict(fallback)
    for key in fallback.keys():
        value = enrichment.get(key) if isinstance(enrichment, dict) else None
        if value is None:
            continue
        if isinstance(fallback[key], list):
            if isinstance(value, list):
                merged[key] = [str(item).strip() for item in value if str(item).strip()][:6]
            elif isinstance(value, str):
                merged[key] = [part.strip() for part in value.split(",") if part.strip()][:6]
            if not merged[key]:
                merged[key] = fallback[key]
        else:
            merged[key] = str(value).strip() or fallback[key]

    # Ensure minimum list lengths for downstream prompt generation quality.
    for list_key, min_items in {
        "key_characters": 3,
        "iconic_scenes": 3,
        "visual_motifs": 3,
        "symbolic_elements": 2,
    }.items():
        values = merged.get(list_key, [])
        if not isinstance(values, list):
            values = []
        while len(values) < min_items:
            values.append(str(fallback[list_key][len(values) % len(fallback[list_key])]))
        merged[list_key] = values

    return merged


def _parse_json_object(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}

    try:
        loaded = json.loads(text)
        return loaded if isinstance(loaded, dict) else {}
    except json.JSONDecodeError:
        pass

    # Recover JSON embedded in markdown/text wrappers.
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return {}
    snippet = match.group(0)
    try:
        loaded = json.loads(snippet)
        return loaded if isinstance(loaded, dict) else {}
    except json.JSONDecodeError:
        return {}


def _merge_usage(
    *,
    usage_path: Path,
    run_usage: UsageCounters,
    enriched_count: int,
    provider: str,
    model: str,
) -> dict[str, Any]:
    existing = _load_json_dict(usage_path)

    total_calls = int(existing.get("total_calls", 0) or 0) + run_usage.total_calls
    total_input = int(existing.get("total_input_tokens", 0) or 0) + run_usage.total_input_tokens
    total_output = int(existing.get("total_output_tokens", 0) or 0) + run_usage.total_output_tokens
    total_cost = float(existing.get("total_cost_usd", 0.0) or 0.0) + run_usage.total_cost_usd

    payload = {
        "total_calls": total_calls,
        "total_input_tokens": total_input,
        "total_output_tokens": total_output,
        "total_cost_usd": round(total_cost, 6),
        "per_book_avg_cost": round((run_usage.total_cost_usd / max(1, enriched_count)), 6),
        "last_run": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "provider": provider,
            "model": model,
            "calls": run_usage.total_calls,
            "input_tokens": run_usage.total_input_tokens,
            "output_tokens": run_usage.total_output_tokens,
            "cost_usd": round(run_usage.total_cost_usd, 6),
            "books_enriched": enriched_count,
        },
    }

    return payload


def _load_descriptions(path: Path) -> dict[str, str]:
    payload = _load_json_dict(path)
    out: dict[str, str] = {}
    for key, value in payload.items():
        out[str(key)] = str(value)
    return out


def _load_json_list(path: Path) -> list[dict[str, Any]]:
    payload = safe_json.load_json(path, [])
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def _load_json_dict(path: Path) -> dict[str, Any]:
    payload = safe_json.load_json(path, {})
    return payload if isinstance(payload, dict) else {}


def _parse_books(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    values: set[int] = set()
    for token in str(raw).split(","):
        part = token.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            lo = _safe_int(start, 0)
            hi = _safe_int(end, 0)
            if lo > 0 and hi > 0:
                for value in range(min(lo, hi), max(lo, hi) + 1):
                    values.add(value)
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Prompt 11A: book metadata enrichment")
    parser.add_argument("--catalog", type=Path, default=config.BOOK_CATALOG_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--books", type=str, default=None, help="Book list/range, e.g. 1,5,8-12")
    parser.add_argument("--force", action="store_true", help="Recompute even if enrichment exists")
    parser.add_argument("--provider", type=str, default=None, help="anthropic|openai")
    parser.add_argument("--model", type=str, default=None)
    parser.add_argument("--max-tokens", type=int, default=None)
    parser.add_argument("--cost-per-1k", type=float, default=None)
    parser.add_argument("--usage-path", type=Path, default=DEFAULT_USAGE_PATH)
    parser.add_argument("--descriptions", type=Path, default=DEFAULT_DESCRIPTIONS_PATH)
    args = parser.parse_args()

    summary = enrich_catalog(
        catalog_path=args.catalog,
        output_path=args.output,
        books=_parse_books(args.books),
        force_refresh=bool(args.force),
        provider=args.provider,
        model=args.model,
        max_tokens=args.max_tokens,
        cost_per_1k_tokens=args.cost_per_1k,
        usage_path=args.usage_path,
        descriptions_path=args.descriptions,
    )
    logger.info("Enrichment summary: %s", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
