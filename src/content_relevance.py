"""Shared content-relevance helpers for Alexandria prompt assembly."""

from __future__ import annotations

import re
from typing import Any

try:
    from src import prompt_generator
except ModuleNotFoundError:  # pragma: no cover
    import prompt_generator  # type: ignore

GENERIC_MARKERS: tuple[str, ...] = (
    "iconic turning point",
    "central protagonist",
    "atmospheric setting moment",
    "atmospheric setting that captures the mood",
    "defining confrontation involving",
    "historically grounded era",
    "circular medallion-ready",
    "pivotal narrative tableau",
    "period costume and historically grounded",
    "symbolic object tied to",
    "dramatic light and weather matching",
    "period-appropriate settings",
    "narrative spaces associated with",
    "classical dramatic tension",
    "narrative consequence",
    "literary depth",
    "a recurring object representing fate",
    "light versus shadow for moral conflict",
    "spatial contrast between confinement and freedom",
    "story-specific props and objects",
    "architectural and environmental details from the book's world",
    "symbolic objects that reinforce the central conflict",
    "primary ally from the narrative",
    "major opposing force in the story",
    "supporting figure tied to the central conflict",
    "{title}",
    "{author}",
    "{scene}",
    "{mood}",
    "{era}",
    "period costume",
    "key characters",
    "supporting cast",
    "mentor/foil",
    "antagonistic force",
)

_PLACEHOLDERS: tuple[str, ...] = ("{title}", "{author}", "{scene}", "{mood}", "{era}", "{subtitle}")
_TITLE_STOPWORDS = {
    "a",
    "an",
    "and",
    "art",
    "book",
    "comedy",
    "crime",
    "divine",
    "history",
    "odyssey",
    "of",
    "on",
    "peace",
    "poetry",
    "prejudice",
    "punishment",
    "republic",
    "sense",
    "the",
    "war",
}
_NAME_STOPWORDS = {
    "Ancient",
    "Edwardian",
    "Egyptian",
    "English",
    "Florentine",
    "Georgian",
    "Greek",
    "Hartfield",
    "Highbury",
    "Italian",
    "Northern",
    "Paris",
    "Regency",
    "Renaissance",
    "Roman",
    "Transylvanian",
    "Tuscan",
    "Verona",
    "Victorian",
}
_ERA_HINTS: tuple[tuple[str, str], ...] = (
    ("regency", "Regency England"),
    ("victorian", "Victorian era"),
    ("edwardian", "Edwardian era"),
    ("renaissance", "Renaissance Europe"),
    ("georgian", "Georgian era"),
    ("tudor", "Tudor England"),
    ("classical", "Classical antiquity"),
    ("roman", "Ancient Rome"),
    ("egyptian", "Ancient Egypt"),
    ("medieval", "Medieval world"),
    ("gothic", "Gothic period atmosphere"),
    ("baroque", "Baroque period"),
)


def normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def prompt_contains_unresolved_placeholders(value: Any) -> bool:
    lower = normalize_text(value).lower()
    return any(token in lower for token in _PLACEHOLDERS)


def is_generic_text(value: Any) -> bool:
    text = normalize_text(value)
    if len(text) < 4:
        if re.fullmatch(r"[A-Z][a-z]{1,3}(?:\s+[A-Z][a-z]{1,3})*", text):
            return False
        return True
    lower = text.lower()
    if any(marker in lower for marker in GENERIC_MARKERS):
        return True
    if re.search(r"\b(main|central)\s+character\b", lower):
        return True
    if len(text) < 8 and text == text.lower():
        return True
    return False


def _iter_strings(value: Any) -> list[str]:
    if isinstance(value, list):
        return [normalize_text(item) for item in value if normalize_text(item)]
    text = normalize_text(value)
    return [text] if text else []


def unique_non_generic_strings(*values: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        for text in _iter_strings(value):
            if is_generic_text(text):
                continue
            token = text.casefold()
            if token in seen:
                continue
            seen.add(token)
            out.append(text)
    return out


def _first_non_generic(*values: Any, default: str = "") -> str:
    rows = unique_non_generic_strings(*values)
    return rows[0] if rows else normalize_text(default)


def _book_enrichment(book: dict[str, Any]) -> dict[str, Any]:
    enrichment = book.get("enrichment", {})
    return enrichment if isinstance(enrichment, dict) else {}


def _extract_named_subject(*values: Any) -> str:
    honorifics = r"(?:Captain|Count|Lady|Lord|Miss|Mr|Mrs|Ms|Dr|Doctor|Sir|Saint|King|Queen|Prince|Princess|Father)"
    pattern = re.compile(
        rf"\b(?:{honorifics}\s+)?[A-Z][a-z]+(?:\s+(?:and|[A-Z][a-z]+))*\b"
    )
    for value in values:
        for text in _iter_strings(value):
            for match in pattern.findall(text):
                token = normalize_text(match)
                if not token:
                    continue
                parts = [part for part in re.split(r"\s+", token) if part]
                if all(part in _NAME_STOPWORDS for part in parts):
                    continue
                if parts and parts[0] in _NAME_STOPWORDS:
                    continue
                return token
    return ""


def _title_as_character(title: str) -> str:
    parts = [part for part in re.findall(r"[A-Za-z][A-Za-z'-]*", normalize_text(title))]
    if not parts:
        return ""
    lowered = [part.lower() for part in parts]
    if len(parts) == 1 and lowered[0] not in _TITLE_STOPWORDS:
        return parts[0]
    if len(parts) >= 3 and lowered[1] == "and" and lowered[0] not in _TITLE_STOPWORDS and lowered[2] not in _TITLE_STOPWORDS:
        return " ".join(parts[:3])
    if len(parts) >= 2 and lowered[1] in {"in", "of"} and lowered[0] not in _TITLE_STOPWORDS:
        return parts[0]
    if parts[0].lower() in {"the", "a", "an"}:
        return ""
    return ""


def _fallback_mood(book: dict[str, Any]) -> str:
    genre = normalize_text(book.get("genre", "")).lower()
    if any(token in genre for token in ("horror", "gothic", "supernatural")):
        return "ominous, haunted, and emotionally intense"
    if any(token in genre for token in ("philosophy", "strategy", "ethics", "logic")):
        return "contemplative, disciplined, and intellectually charged"
    if any(token in genre for token in ("religious", "biblical", "spiritual", "apocryphal")):
        return "sacred, reverent, and luminous"
    if any(token in genre for token in ("romance", "literature", "novel", "drama", "poetry")):
        return "emotionally resonant, intimate, and dramatic"
    if any(token in genre for token in ("occult", "mystical", "esoteric", "myth")):
        return "mysterious, visionary, and symbolically charged"
    return "dramatic, literary, and historically grounded"


def _fallback_era(book: dict[str, Any], motif: Any) -> str:
    book_era = normalize_text(book.get("era", ""))
    if book_era and not is_generic_text(book_era):
        return book_era
    year = normalize_text(book.get("year", ""))
    if year:
        return f"{year} period setting"
    style_hint = normalize_text(getattr(motif, "style_specific_prefix", ""))
    lower = style_hint.lower()
    for marker, era in _ERA_HINTS:
        if marker in lower:
            return era
    setting = normalize_text(book.get("setting", "")) or normalize_text(_book_enrichment(book).get("setting_primary", ""))
    if setting and not is_generic_text(setting):
        return setting
    return "historically appropriate to the source text"


def _fallback_scene(*, title: str, author: str, protagonist: str, setting: str, era: str, genre: str) -> str:
    location = setting or era or genre or "the book's defining world"
    actor = protagonist or "the book's central figures"
    creator = f" by {author}" if author else ""
    return f'A decisive scene from "{title}"{creator} set in {location}, focused on {actor}.'


def inject_protagonist(scene: Any, protagonist: Any) -> str:
    base_scene = normalize_text(scene)
    main_character = normalize_text(protagonist)
    if not base_scene:
        return ""
    if not main_character or is_generic_text(main_character):
        return base_scene
    lower_scene = base_scene.lower()
    lower_character = main_character.lower()
    if lower_character in lower_scene:
        return base_scene
    subject_label = "The main characters shown are" if " and " in lower_character else "The main character shown is"
    return f"{base_scene}. {subject_label} {main_character}."


def resolve_prompt_context(book: dict[str, Any]) -> dict[str, Any]:
    title = normalize_text(book.get("title", ""))
    author = normalize_text(book.get("author", ""))
    enrichment = _book_enrichment(book)
    motif = prompt_generator._motif_for_book(book)  # type: ignore[attr-defined]

    protagonist = _first_non_generic(
        enrichment.get("protagonist"),
        enrichment.get("key_characters", []),
        default="",
    )
    if not protagonist:
        protagonist = _title_as_character(title)
    if not protagonist:
        protagonist = _extract_named_subject(
            getattr(motif, "character_portrait", ""),
            getattr(motif, "iconic_scene", ""),
            getattr(motif, "dramatic_moment", ""),
        )

    scene_pool = unique_non_generic_strings(
        enrichment.get("iconic_scenes", []),
        enrichment.get("scene"),
        getattr(motif, "iconic_scene", ""),
        getattr(motif, "dramatic_moment", ""),
        getattr(motif, "setting_landscape", ""),
    )

    setting = _first_non_generic(
        enrichment.get("setting_primary"),
        enrichment.get("setting"),
        getattr(motif, "setting_landscape", ""),
        default="",
    )
    mood = _first_non_generic(
        enrichment.get("emotional_tone"),
        enrichment.get("mood"),
        enrichment.get("tones", []),
        default=_fallback_mood(book),
    )
    era = _first_non_generic(
        enrichment.get("era"),
        default=_fallback_era(book, motif),
    )
    genre = normalize_text(enrichment.get("genre", "")) or normalize_text(book.get("genre", ""))
    scene = scene_pool[0] if scene_pool else ""
    if not scene:
        scene = _fallback_scene(
            title=title or "the book",
            author=author,
            protagonist=protagonist,
            setting=setting,
            era=era,
            genre=genre,
        )
        scene_pool = [scene]

    return {
        "title": title,
        "author": author,
        "scene": scene,
        "scene_with_protagonist": inject_protagonist(scene, protagonist),
        "scene_pool": scene_pool,
        "mood": mood,
        "era": era,
        "protagonist": protagonist,
        "setting": setting,
        "genre": genre,
    }


def ensure_prompt_book_context(
    *,
    prompt: Any,
    book: dict[str, Any],
    require_scene_anchor: bool = False,
) -> str:
    text = normalize_text(prompt)
    context = resolve_prompt_context(book)
    title = context["title"]
    author = context["author"]
    title_lower = title.lower()
    author_lower = author.lower()
    text_lower = text.lower()
    has_book_reference = bool(title_lower and title_lower in text_lower) or bool(author_lower and author_lower in text_lower)

    if not has_book_reference and title:
        context_prefix = f"Illustration for '{title}'"
        if author:
            context_prefix = f"{context_prefix} by {author}"
        text = f"{context_prefix}. {text}".strip()
        text_lower = text.lower()

    needs_scene_anchor = require_scene_anchor or is_generic_text(text[:320]) or prompt_contains_unresolved_placeholders(text)
    scene_anchor = normalize_text(context.get("scene_with_protagonist") or context.get("scene"))
    if scene_anchor and needs_scene_anchor and "critical scene requirement" not in text_lower:
        text = f"CRITICAL SCENE REQUIREMENT: the illustration must specifically depict {scene_anchor}. {text}".strip()

    return normalize_text(text)
