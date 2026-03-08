"""Prompt library system for reusable, title-agnostic generation prompts (Prompt 2A)."""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

try:
    from src import config
    from src import safe_json
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import safe_json  # type: ignore
    from logger import get_logger  # type: ignore

logger = get_logger(__name__)

SUPPORTED_REUSABLE_PLACEHOLDERS: tuple[str, ...] = (
    "{title}",
    "{author}",
    "{TITLE}",
    "{AUTHOR}",
    "{SUBTITLE}",
    "{SCENE}",
    "{MOOD}",
    "{ERA}",
)

ALEXANDRIA_SYSTEM_NEGATIVE_PROMPT = (
    "No text, no letters, no words, no numbers, no titles, no author names, no typography, no captions, "
    "no labels, no watermarks, no signatures, no inscriptions of any kind. No modern elements, no photography, "
    "no 3D rendering, no digital art aesthetic, no gradients on background, no neon colours, no sans-serif fonts, "
    "no minimalist design, no stock photo look, no cartoonish style, no anime influence, no spelling mistakes, "
    "no blurry medallion illustration, no off-centre composition, no white or light backgrounds."
)

ALEXANDRIA_PROMPT_SPECS: tuple[dict[str, object], ...] = (
    {
        "id": "alexandria-base-classical-devotion",
        "name": "BASE 1 — Classical Devotion",
        "prompt_template": (
            "Book cover illustration only — no text, no title, no author name, no lettering of any kind. "
            "Deep navy blue background (#0A1628). Ornate gold baroque scrollwork border frame with floral filigree along all four edges. "
            "Centered circular medallion illustration in the style of Byzantine iconography and Renaissance religious painting: {SCENE}. "
            "The illustration is framed by a thick gold circular border with decorative notching. "
            "Leave clear empty space above and below the medallion where text will be composited later. "
            "Small gold ornamental dividers as decorative accents. "
            "The overall mood is {MOOD}. Era reference: {ERA}. Classical, sacred, museum-quality. "
            "No modern elements. Aspect ratio 6:9, high resolution, print-ready."
        ),
        "notes": "Alexandria three-part formula prompt. Best for: Religious, Apocryphal, Biblical.",
        "tags": ["alexandria", "base", "classical-devotion", "religious", "apocryphal", "biblical"],
    },
    {
        "id": "alexandria-base-philosophical-gravitas",
        "name": "BASE 2 — Philosophical Gravitas",
        "prompt_template": (
            "Book cover illustration only — no text, no title, no author name, no lettering of any kind. "
            "Deep navy blue background (#0A1628). Clean gold geometric border frame with Art Deco influence — straight lines, symmetrical corners, "
            "subtle Greek key pattern along edges. Centered circular medallion illustration in the style of neoclassical painting: {SCENE}. "
            "The illustration shows balanced composition with architectural or natural elements evoking contemplation. "
            "Framed by a refined gold circular border. Leave clear empty space above and below the medallion where text will be composited later. "
            "Minimal ornamental dividers — single gold rules. The mood is {MOOD}. Era reference: {ERA}. "
            "Intellectual, timeless, restrained elegance. Aspect ratio 6:9, high resolution, print-ready."
        ),
        "notes": "Alexandria three-part formula prompt. Best for: Philosophy, Self-Help, Strategy.",
        "tags": ["alexandria", "base", "philosophical-gravitas", "philosophy", "self-help", "strategy"],
    },
    {
        "id": "alexandria-base-gothic-atmosphere",
        "name": "BASE 3 — Gothic Atmosphere",
        "prompt_template": (
            "Book cover illustration only — no text, no title, no author name, no lettering of any kind. "
            "Deep navy-black background (#080F1D). Heavy ornate gold gothic frame with pointed arch motifs, thorned vine scrollwork, "
            "and subtle skull or gargoyle corner embellishments. Centered circular medallion illustration in the style of dark Romantic-era painting "
            "with dramatic chiaroscuro lighting: {SCENE}. The illustration emphasizes shadow, supernatural atmosphere, and Victorian-era dread. "
            "Framed by an elaborate gold circular border with thorn or chain detailing. "
            "Leave clear empty space above and below the medallion where text will be composited later. "
            "Gothic ornamental dividers — small bat-wing or fleuron motifs. The mood is {MOOD}. Era reference: {ERA}. "
            "Dark, atmospheric, hauntingly beautiful. Aspect ratio 6:9, high resolution, print-ready."
        ),
        "notes": "Alexandria three-part formula prompt. Best for: Horror, Gothic, Supernatural.",
        "tags": ["alexandria", "base", "gothic-atmosphere", "horror", "gothic", "supernatural"],
    },
    {
        "id": "alexandria-base-romantic-realism",
        "name": "BASE 4 — Romantic Realism",
        "prompt_template": (
            "Book cover illustration only — no text, no title, no author name, no lettering of any kind. "
            "Deep navy blue background (#0F1B33). Elegant gold classical frame with balanced symmetry — laurel wreath corners, "
            "subtle column or pilaster motifs along vertical edges, simple rule along top and bottom. "
            "Centered circular medallion illustration in the style of 19th-century Romantic realism and academic painting: {SCENE}. "
            "Rich colour palette within the medallion — warm earth tones, dramatic skies, detailed period clothing. "
            "Framed by a refined gold circular border. Leave clear empty space above and below the medallion where text will be composited later. "
            "Elegant gold fleuron dividers. The mood is {MOOD}. Era reference: {ERA}. "
            "Literary, emotionally resonant, painterly. Aspect ratio 6:9, high resolution, print-ready."
        ),
        "notes": "Alexandria three-part formula prompt. Best for: Classical Literature, Novels, Drama.",
        "tags": ["alexandria", "base", "romantic-realism", "literature", "novels", "drama"],
    },
    {
        "id": "alexandria-base-esoteric-mysticism",
        "name": "BASE 5 — Esoteric Mysticism",
        "prompt_template": (
            "Book cover illustration only — no text, no title, no author name, no lettering of any kind. "
            "Deep navy blue background (#0A1628) with very subtle dark celestial texture (barely visible star field). "
            "Intricate gold border frame combining alchemical symbols, sacred geometry patterns, and esoteric motifs — pentagrams, ouroboros elements, celestial circles. "
            "Centered circular medallion illustration in the style of medieval illuminated manuscripts crossed with Romantic-era mystical painting: {SCENE}. "
            "The illustration features mystical light sources, symbolic objects, and arcane atmosphere. "
            "Framed by a gold circular border with astrological or alchemical notations. "
            "Leave clear empty space above and below the medallion where text will be composited later. "
            "Esoteric ornamental dividers — small pentacle or celestial motifs. The mood is {MOOD}. Era reference: {ERA}. "
            "Mysterious, ancient, forbidden knowledge. Aspect ratio 6:9, high resolution, print-ready."
        ),
        "notes": "Alexandria three-part formula prompt. Best for: Occult, Mystical, Forbidden Texts.",
        "tags": ["alexandria", "base", "esoteric-mysticism", "occult", "mystical", "esoteric"],
    },
    {
        "id": "alexandria-wildcard-edo-meets-alexandria",
        "name": "WILDCARD 1 — Edo Meets Alexandria",
        "prompt_template": (
            "Book cover illustration only — no text, no title, no author name, no lettering of any kind. "
            "Deep navy blue background (#0A1628). Gold ornamental border frame with Japanese-influenced geometric patterns — interlocking hexagons, wave motifs (seigaiha), "
            "and cloud scrolls rendered in gold leaf style. Centered circular medallion illustration painted in the style of Katsushika Hokusai meets classical European oil painting: {SCENE}, "
            "rendered with ukiyo-e compositional principles but Western chiaroscuro lighting. "
            "Framed by a gold circular border with wave-pattern detailing. Leave clear empty space above and below the medallion where text will be composited later. "
            "Gold ornamental dividers using stylised cloud motifs. The mood is {MOOD}. Era reference: {ERA}. "
            "Aspect ratio 6:9, high resolution, print-ready."
        ),
        "notes": "Alexandria wildcard prompt. Japanese woodblock elegance fused with classical oil storytelling.",
        "tags": ["alexandria", "wildcard", "edo-meets-alexandria", "japanese", "ukiyoe"],
    },
    {
        "id": "alexandria-wildcard-pre-raphaelite-garden",
        "name": "WILDCARD 2 — Pre-Raphaelite Garden",
        "prompt_template": (
            "Book cover illustration only — no text, no title, no author name, no lettering of any kind. "
            "Deep navy blue background (#0A1628). Gold Art Nouveau border frame — sinuous organic lines, flowing botanical forms (iris, lily, vine tendrils) "
            "in the style of Alphonse Mucha, rendered entirely in gold on navy. "
            "Centered circular medallion illustration in the Pre-Raphaelite painting style: {SCENE}, with luminous skin tones, flowing drapery, rich botanical details, "
            "and stained-glass-window quality light. Framed by a gold circular border with intertwined botanical motifs. "
            "Leave clear empty space above and below the medallion where text will be composited later. "
            "Art Nouveau floral dividers. The mood is {MOOD}. Era reference: {ERA}. Aspect ratio 6:9, high resolution, print-ready."
        ),
        "notes": "Alexandria wildcard prompt. Mucha framing plus Pre-Raphaelite colour for lush literary covers.",
        "tags": ["alexandria", "wildcard", "pre-raphaelite-garden", "pre-raphaelite", "art-nouveau"],
    },
    {
        "id": "alexandria-wildcard-illuminated-manuscript",
        "name": "WILDCARD 3 — Illuminated Manuscript",
        "prompt_template": (
            "Book cover illustration only — no text, no title, no author name, no lettering of any kind. "
            "Deep navy blue background (#0A1628). Gold border frame in the style of medieval illuminated manuscript marginalia — intricate interlace knotwork (Book of Kells style), "
            "inhabited vine scrolls with tiny gold animals and figures woven into the borders. "
            "Centered circular medallion illustration in the style of a hand-painted medieval miniature with gold leaf highlights: {SCENE}, "
            "rendered with flat perspective, vivid colours on gold ground, and meticulous decorative detail. "
            "Framed by a Celtic knotwork gold circular border. Leave clear empty space above and below the medallion where text will be composited later. "
            "Celtic knot dividers. The mood is {MOOD}. Era reference: {ERA}. Aspect ratio 6:9, high resolution, print-ready."
        ),
        "notes": "Alexandria wildcard prompt. Medieval manuscript energy for ancient or sacred material.",
        "tags": ["alexandria", "wildcard", "illuminated-manuscript", "medieval", "celtic"],
    },
    {
        "id": "alexandria-wildcard-celestial-cartography",
        "name": "WILDCARD 4 — Celestial Cartography",
        "prompt_template": (
            "Book cover illustration only — no text, no title, no author name, no lettering of any kind. "
            "Deep navy blue background (#0A1628). Gold border frame with celestial cartography motifs — constellation lines, zodiac symbols, orbital paths, "
            "and compass roses rendered in fine gold engraving style. "
            "Centered circular medallion illustration in the style of astronomical engravings from Harmonia Macrocosmica crossed with Romantic landscape painting: {SCENE}, "
            "with dramatic celestial elements — star fields, eclipses, planetary alignments, or aurora-like light integrated into the composition. "
            "Framed by a gold circular border designed as an astrolabe or armillary sphere. "
            "Leave clear empty space above and below the medallion where text will be composited later. "
            "Gold star-and-compass dividers. The mood is {MOOD}. Era reference: {ERA}. Aspect ratio 6:9, high resolution, print-ready."
        ),
        "notes": "Alexandria wildcard prompt. Cosmic engraving language for knowledge-rich or metaphysical titles.",
        "tags": ["alexandria", "wildcard", "celestial-cartography", "celestial", "astronomy"],
    },
    {
        "id": "alexandria-wildcard-temple-of-knowledge",
        "name": "WILDCARD 5 — Temple of Knowledge",
        "prompt_template": (
            "Book cover illustration only — no text, no title, no author name, no lettering of any kind. "
            "Deep navy blue background (#0A1628). Gold border frame in the style of ancient Egyptian temple relief carving — papyrus columns, lotus capitals, "
            "winged sun disc at top center, ankh and djed pillar motifs along sides, all rendered as raised gold relief on navy. "
            "Centered circular medallion illustration combining Egyptian tomb painting flatness with Orientalist oil painting richness: {SCENE}, "
            "rendered with the bold outlines and profile views of Egyptian art but the colour depth and atmospheric lighting of 19th-century Orientalist masters. "
            "Framed by a gold cartouche-style circular border. Leave clear empty space above and below the medallion where text will be composited later. "
            "Gold scarab or lotus dividers. The mood is {MOOD}. Era reference: {ERA}. Aspect ratio 6:9, high resolution, print-ready."
        ),
        "notes": "Alexandria wildcard prompt. Direct homage to Alexandria's Egyptian origin and temple symbolism.",
        "tags": ["alexandria", "wildcard", "temple-of-knowledge", "egyptian", "mystical"],
    },
)


@dataclass(slots=True)
class StyleAnchor:
    """A reusable style component that can be mixed into prompts."""

    name: str
    description: str
    style_text: str
    tags: list[str]


@dataclass(slots=True)
class LibraryPrompt:
    """A saved prompt that worked well."""

    id: str
    name: str
    prompt_template: str
    style_anchors: list[str]
    negative_prompt: str
    source_book: str
    source_model: str
    quality_score: float
    saved_by: str
    created_at: str
    notes: str
    tags: list[str]
    category: str = "general"
    version: int = 1
    usage_count: int = 0
    win_count: int = 0
    last_used_at: str = ""
    updated_at: str = ""


class PromptLibrary:
    """Manages style anchors and saved prompts for single-cover iteration and bulk runs."""

    def __init__(self, library_path: Path):
        self.library_path = library_path
        self.library_path.parent.mkdir(parents=True, exist_ok=True)
        self._style_anchors: dict[str, StyleAnchor] = {}
        self._prompts: dict[str, LibraryPrompt] = {}
        self._versions: dict[str, list[dict[str, object]]] = {}
        self._load_or_seed()

    def get_style_anchors(self) -> list[StyleAnchor]:
        """Return all available style anchors."""
        return sorted(self._style_anchors.values(), key=lambda anchor: anchor.name)

    def save_prompt(self, prompt: LibraryPrompt) -> None:
        """Save a successful prompt to the library."""
        _validate_prompt_template(prompt.prompt_template)
        existing = self._prompts.get(prompt.id)
        normalized = LibraryPrompt(
            id=str(prompt.id),
            name=str(prompt.name or prompt.id),
            prompt_template=str(prompt.prompt_template),
            style_anchors=[str(anchor).strip() for anchor in list(prompt.style_anchors)],
            negative_prompt=str(prompt.negative_prompt or ""),
            source_book=str(prompt.source_book or ""),
            source_model=str(prompt.source_model or ""),
            quality_score=float(prompt.quality_score or 0.0),
            saved_by=str(prompt.saved_by or "auto"),
            created_at=str(prompt.created_at or _utc_now()),
            notes=str(prompt.notes or ""),
            tags=[str(tag).strip() for tag in list(prompt.tags)],
            category=str(getattr(prompt, "category", "general") or "general"),
            version=max(1, int(getattr(prompt, "version", 1) or 1)),
            usage_count=max(0, int(getattr(prompt, "usage_count", 0) or 0)),
            win_count=max(0, int(getattr(prompt, "win_count", 0) or 0)),
            last_used_at=str(getattr(prompt, "last_used_at", "") or ""),
            updated_at=str(getattr(prompt, "updated_at", "") or _utc_now()),
        )
        if existing is not None:
            history = self._versions.setdefault(str(prompt.id), [])
            history.append(asdict(existing))
            normalized.version = max(1, int(existing.version or 1) + 1)
            if len(history) > 100:
                del history[:-100]
        self._prompts[prompt.id] = normalized
        self._persist()
        logger.info(
            "Saved prompt to library",
            extra={"prompt_id": prompt.id, "prompt_name": prompt.name, "score": prompt.quality_score},
        )

    def get_prompts(self, tags: list[str] | None = None) -> list[LibraryPrompt]:
        """Get prompts, optionally filtered by tags."""
        values = list(self._prompts.values())
        if tags:
            wanted = {tag.strip().lower() for tag in tags if tag.strip()}
            values = [
                prompt
                for prompt in values
                if wanted.intersection({tag.lower() for tag in prompt.tags})
                or wanted.intersection({anchor.lower() for anchor in prompt.style_anchors})
            ]
        return _sorted_prompts(values)

    def get_prompt(self, prompt_id: str) -> LibraryPrompt | None:
        """Return one prompt by id."""
        return self._prompts.get(str(prompt_id))

    def get_versions(self, prompt_id: str) -> list[dict[str, object]]:
        """Return historical versions for one prompt id."""
        rows = self._versions.get(str(prompt_id), [])
        return list(rows)

    def update_prompt(self, prompt_id: str, **updates: object) -> LibraryPrompt:
        """Update one prompt and persist previous state to versions history."""
        token = str(prompt_id)
        current = self._prompts.get(token)
        if current is None:
            raise KeyError(prompt_id)

        history = self._versions.setdefault(token, [])
        history.append(asdict(current))
        if len(history) > 100:
            del history[:-100]

        name = str(updates.get("name", current.name) or current.name).strip() or current.name
        prompt_template = str(updates.get("prompt_template", current.prompt_template) or current.prompt_template).strip() or current.prompt_template
        _validate_prompt_template(prompt_template)

        style_anchors = updates.get("style_anchors", current.style_anchors)
        tags = updates.get("tags", current.tags)
        updated = LibraryPrompt(
            id=current.id,
            name=name,
            prompt_template=prompt_template,
            style_anchors=[str(anchor).strip() for anchor in style_anchors] if isinstance(style_anchors, list) else list(current.style_anchors),
            negative_prompt=str(updates.get("negative_prompt", current.negative_prompt) or current.negative_prompt),
            source_book=str(updates.get("source_book", current.source_book) or current.source_book),
            source_model=str(updates.get("source_model", current.source_model) or current.source_model),
            quality_score=float(updates.get("quality_score", current.quality_score) or current.quality_score),
            saved_by=str(updates.get("saved_by", current.saved_by) or current.saved_by),
            created_at=current.created_at,
            notes=str(updates.get("notes", current.notes) or current.notes),
            tags=[str(tag).strip() for tag in tags] if isinstance(tags, list) else list(current.tags),
            category=str(updates.get("category", current.category) or current.category or "general"),
            version=max(1, int(current.version or 1) + 1),
            usage_count=max(0, int(updates.get("usage_count", current.usage_count) or current.usage_count)),
            win_count=max(0, int(updates.get("win_count", current.win_count) or current.win_count)),
            last_used_at=str(updates.get("last_used_at", current.last_used_at) or current.last_used_at),
            updated_at=_utc_now(),
        )
        self._prompts[token] = updated
        self._persist()
        return updated

    def delete_prompt(self, prompt_id: str) -> bool:
        """Delete one prompt by id."""
        token = str(prompt_id)
        if token not in self._prompts:
            return False
        self._prompts.pop(token, None)
        self._versions.pop(token, None)
        self._persist()
        return True

    def record_usage(self, prompt_id: str, *, won: bool = False) -> LibraryPrompt:
        """Increment usage and optional win counters for one prompt."""
        token = str(prompt_id)
        current = self._prompts.get(token)
        if current is None:
            raise KeyError(prompt_id)
        updated = LibraryPrompt(
            id=current.id,
            name=current.name,
            prompt_template=current.prompt_template,
            style_anchors=list(current.style_anchors),
            negative_prompt=current.negative_prompt,
            source_book=current.source_book,
            source_model=current.source_model,
            quality_score=current.quality_score,
            saved_by=current.saved_by,
            created_at=current.created_at,
            notes=current.notes,
            tags=list(current.tags),
            category=current.category,
            version=max(1, int(current.version or 1)),
            usage_count=max(0, int(current.usage_count or 0) + 1),
            win_count=max(0, int(current.win_count or 0) + (1 if won else 0)),
            last_used_at=_utc_now(),
            updated_at=_utc_now(),
        )
        self._prompts[token] = updated
        self._persist()
        return updated

    def search_prompts(
        self,
        query: str = "",
        tags: Sequence[str] | None = None,
        min_quality: float = 0.0,
    ) -> list[LibraryPrompt]:
        """Search prompts by text, tags, and minimum quality score."""
        query_tokens = {token for token in query.lower().split() if token}
        tag_tokens = {token.lower() for token in tags} if tags else set()

        def _matches(prompt: LibraryPrompt) -> bool:
            if prompt.quality_score < min_quality:
                return False

            blob = " ".join(
                [
                    prompt.name,
                    prompt.prompt_template,
                    prompt.notes,
                    " ".join(prompt.tags),
                    " ".join(prompt.style_anchors),
                ]
            ).lower()

            if query_tokens and not all(token in blob for token in query_tokens):
                return False

            if tag_tokens:
                prompt_tokens = {tag.lower() for tag in prompt.tags}.union(
                    {anchor.lower() for anchor in prompt.style_anchors}
                )
                if not tag_tokens.intersection(prompt_tokens):
                    return False
            return True

        return _sorted_prompts(prompt for prompt in self._prompts.values() if _matches(prompt))

    def build_prompt(self, book_title: str, style_anchors: list[str], custom_text: str = "") -> str:
        """Build a prompt from style anchors + book title + optional custom text."""
        selected = [self._style_anchors[name] for name in style_anchors if name in self._style_anchors]
        if not selected:
            raise ValueError("At least one valid style anchor is required to build a prompt.")

        style_text = ", ".join(anchor.style_text for anchor in selected)
        custom_part = f" {custom_text.strip()}" if custom_text.strip() else ""
        prompt = (
            f"Create a circular medallion illustration for \"{book_title}\" showing the most iconic "
            f"scene or symbolic moment from the story.{custom_part} {style_text}"
        )
        return " ".join(prompt.split())

    def get_best_prompts_for_bulk(self, top_n: int = 5) -> list[LibraryPrompt]:
        """Get top-N prompts by quality score for bulk processing."""
        ordered = sorted(self._prompts.values(), key=lambda prompt: prompt.quality_score, reverse=True)
        return ordered[: max(1, top_n)]

    def add_style_anchor(self, anchor: StyleAnchor) -> None:
        """Add or update a style anchor."""
        self._style_anchors[anchor.name] = anchor
        self._persist()
        logger.info("Upserted style anchor", extra={"anchor": anchor.name})

    def _load_or_seed(self) -> None:
        changed = False
        if self.library_path.exists():
            self._load()
            changed = self._ensure_alexandria_prompts() or changed
            if self._style_anchors and self._prompts:
                if changed:
                    self._persist()
                return

        anchors, prompts = self._seed_library()
        self._style_anchors = {anchor.name: anchor for anchor in anchors}
        self._prompts = {prompt.id: prompt for prompt in prompts}
        self._ensure_alexandria_prompts()
        self._persist()

    def _load(self) -> None:
        payload = safe_json.load_json(self.library_path, {})
        if not isinstance(payload, dict):
            payload = {}
        style_payload = payload.get("style_anchors", [])
        prompts_payload = payload.get("prompts", [])

        anchors: dict[str, StyleAnchor] = {}
        for item in style_payload if isinstance(style_payload, list) else []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            if not name:
                continue
            tags_raw = item.get("tags", [])
            tags = [str(tag).strip() for tag in tags_raw] if isinstance(tags_raw, list) else []
            anchors[name] = StyleAnchor(
                name=name,
                description=str(item.get("description", "")).strip(),
                style_text=str(item.get("style_text", "")).strip(),
                tags=[tag for tag in tags if tag],
            )
        self._style_anchors = anchors

        prompts: dict[str, LibraryPrompt] = {}
        for item in prompts_payload if isinstance(prompts_payload, list) else []:
            if not isinstance(item, dict):
                continue
            prompt_id = str(item.get("id", "")).strip()
            template = str(item.get("prompt_template", "")).strip()
            if not prompt_id or not template:
                continue
            style_raw = item.get("style_anchors", [])
            tags_raw = item.get("tags", [])
            prompts[prompt_id] = LibraryPrompt(
                id=prompt_id,
                name=str(item.get("name", "")).strip() or prompt_id,
                prompt_template=template,
                style_anchors=[str(anchor).strip() for anchor in style_raw] if isinstance(style_raw, list) else [],
                negative_prompt=str(item.get("negative_prompt", "")).strip(),
                source_book=str(item.get("source_book", "")).strip(),
                source_model=str(item.get("source_model", "")).strip(),
                quality_score=float(item.get("quality_score", 0.0) or 0.0),
                saved_by=str(item.get("saved_by", "auto") or "auto"),
                created_at=str(item.get("created_at", _utc_now()) or _utc_now()),
                notes=str(item.get("notes", "")).strip(),
                tags=[str(tag).strip() for tag in tags_raw] if isinstance(tags_raw, list) else [],
                category=str(item.get("category", "general") or "general"),
                version=max(1, int(item.get("version", 1) or 1)),
                usage_count=max(0, int(item.get("usage_count", 0) or 0)),
                win_count=max(0, int(item.get("win_count", 0) or 0)),
                last_used_at=str(item.get("last_used_at", "") or ""),
                updated_at=str(item.get("updated_at", "") or ""),
            )
        self._prompts = prompts

        versions_payload = payload.get("versions", {})
        versions: dict[str, list[dict[str, object]]] = {}
        if isinstance(versions_payload, dict):
            for key, rows in versions_payload.items():
                if not isinstance(rows, list):
                    continue
                cleaned = [dict(item) for item in rows if isinstance(item, dict)]
                if cleaned:
                    versions[str(key)] = cleaned[-100:]
        self._versions = versions

    def _persist(self) -> None:
        payload = {
            "version": 2,
            "updated_at": _utc_now(),
            "style_anchors": [asdict(anchor) for anchor in self.get_style_anchors()],
            "prompts": [
                asdict(prompt)
                for prompt in _sorted_prompts(self._prompts.values())
            ],
            "versions": {prompt_id: rows for prompt_id, rows in self._versions.items()},
        }
        safe_json.atomic_write_json(self.library_path, payload)
        logger.debug("Persisted prompt library", extra={"path": str(self.library_path), "anchors": len(self._style_anchors), "prompts": len(self._prompts)})

    def _ensure_alexandria_prompts(self) -> bool:
        changed = False
        existing_by_name = {str(prompt.name).strip().lower(): prompt for prompt in self._prompts.values()}
        for spec in ALEXANDRIA_PROMPT_SPECS:
            prompt_id = str(spec.get("id", "")).strip()
            name = str(spec.get("name", "")).strip()
            if not prompt_id or not name:
                continue
            current = self._prompts.get(prompt_id) or existing_by_name.get(name.lower())
            if current is not None:
                continue
            created_at = _utc_now()
            prompt = LibraryPrompt(
                id=prompt_id,
                name=name,
                prompt_template=str(spec.get("prompt_template", "")).strip(),
                style_anchors=[],
                negative_prompt=ALEXANDRIA_SYSTEM_NEGATIVE_PROMPT,
                source_book="builtin",
                source_model="openrouter/google/gemini-3-pro-image-preview",
                quality_score=1.0,
                saved_by="system",
                created_at=created_at,
                notes=str(spec.get("notes", "")).strip(),
                tags=list(spec.get("tags", [])) if isinstance(spec.get("tags", []), list) else ["alexandria"],
                category="builtin",
                version=1,
                usage_count=0,
                win_count=0,
                last_used_at="",
                updated_at=created_at,
            )
            self._prompts[prompt.id] = prompt
            existing_by_name[name.lower()] = prompt
            changed = True
        return changed

    def _seed_library(self) -> tuple[list[StyleAnchor], list[LibraryPrompt]]:
        templates = safe_json.load_json(config.PROMPT_TEMPLATES_PATH, {})
        if not isinstance(templates, dict):
            templates = {}
        negative_prompt = templates.get("negative_prompt", "")

        style_groups = templates.get("style_groups", {})
        sketch_text = style_groups.get("sketch_style", {}).get("style_anchors", "")
        oil_text = style_groups.get("oil_painting_style", {}).get("style_anchors", "")
        alt_text = style_groups.get("alternative_style", {}).get("style_anchors", "")

        anchors = [
            StyleAnchor(
                name="warm_sepia_sketch",
                description="Hand-drawn 19th-century sketch aesthetic with sepia warmth.",
                style_text=sketch_text or "classical pen-and-ink sketch, sepia tones, crosshatching",
                tags=["sketch", "sepia", "classical"],
            ),
            StyleAnchor(
                name="engraving_detailed",
                description="Ultra-detailed copperplate engraving and etching line work.",
                style_text="copper plate engraving, fine line work, meticulous etching detail, circular vignette composition",
                tags=["engraving", "detailed", "linework"],
            ),
            StyleAnchor(
                name="dramatic_oil",
                description="Classical oil painting with dramatic light and depth.",
                style_text=oil_text or "classical oil painting, dramatic chiaroscuro, rich brushwork",
                tags=["oil", "dramatic", "classical"],
            ),
            StyleAnchor(
                name="gothic_moody",
                description="Dark romantic mood with atmospheric shadows.",
                style_text="gothic romantic atmosphere, moody shadows, dramatic rim lighting, painterly depth",
                tags=["gothic", "moody", "dramatic"],
            ),
            StyleAnchor(
                name="watercolor_soft",
                description="Soft watercolor washes with delicate transitions.",
                style_text="soft watercolor washes, delicate brush texture, gentle tonal transitions, classical composition",
                tags=["watercolor", "soft", "pastoral"],
            ),
            StyleAnchor(
                name="allegorical_symbolic",
                description="Symbolic visual storytelling with period-appropriate motifs.",
                style_text=alt_text or "period-appropriate artistic style, allegorical symbolism, hand-crafted aesthetic",
                tags=["symbolic", "allegory", "alternative"],
            ),
        ]

        starter_prompt_specs = [
            {
                "name": "Iconic Scene Sketch Baseline",
                "template": "Detailed pen-and-ink engraving of the most iconic scene from {title}, rendered as a circular vignette.",
                "anchors": ["warm_sepia_sketch", "engraving_detailed"],
                "model": "flux-2-pro",
                "score": 0.80,
                "tags": ["iconic", "sketch", "baseline"],
                "notes": "Strong baseline for classical literary titles.",
            },
            {
                "name": "Character Portrait Etching",
                "template": "Classical engraved portrait of the central character from {title}, with expressive posture and period costume.",
                "anchors": ["warm_sepia_sketch", "engraving_detailed"],
                "model": "gpt-image-1-medium",
                "score": 0.78,
                "tags": ["portrait", "character", "sketch"],
                "notes": "Works well when character focus is more recognizable than scene focus.",
            },
            {
                "name": "Setting-Led Sketch Landscape",
                "template": "19th-century style book illustration of the defining setting from {title}, emphasizing depth and architectural detail.",
                "anchors": ["warm_sepia_sketch", "engraving_detailed"],
                "model": "flux-2-schnell",
                "score": 0.73,
                "tags": ["setting", "landscape", "sketch"],
                "notes": "Fast and cheap exploration pattern for geography-heavy books.",
            },
            {
                "name": "Dramatic Chiaroscuro Moment",
                "template": "Masterpiece-style classical oil painting of the pivotal dramatic moment in {title}, with cinematic light contrast.",
                "anchors": ["dramatic_oil"],
                "model": "gpt-image-1-high",
                "score": 0.84,
                "tags": ["dramatic", "oil", "high_quality"],
                "notes": "High-end quality ceiling prompt for shortlist comparisons.",
            },
            {
                "name": "Gothic Tension Scene",
                "template": "Atmospheric gothic interpretation of the most psychologically intense moment from {title}.",
                "anchors": ["dramatic_oil", "gothic_moody"],
                "model": "imagen-4-ultra",
                "score": 0.81,
                "tags": ["gothic", "mood", "dramatic"],
                "notes": "Useful for darker classics and tragic narratives.",
            },
            {
                "name": "Soft Symbolic Watercolor",
                "template": "Symbolic watercolor composition for {title}, using allegorical objects to represent the book's core themes.",
                "anchors": ["watercolor_soft", "allegorical_symbolic"],
                "model": "imagen-4-fast",
                "score": 0.75,
                "tags": ["symbolic", "watercolor", "alternative"],
                "notes": "Good fallback when literal scenes feel crowded inside circular crops.",
            },
            {
                "name": "Baroque Allegory",
                "template": "Classical allegorical illustration for {title}, with period motifs and layered symbolic storytelling.",
                "anchors": ["allegorical_symbolic", "engraving_detailed"],
                "model": "nano-banana-pro",
                "score": 0.76,
                "tags": ["allegory", "symbolic", "baroque"],
                "notes": "Balances uniqueness with consistency for batch candidates.",
            },
            {
                "name": "Heroic Oil Tableau",
                "template": "Heroic classical oil tableau depicting the defining confrontation from {title}, with rich golden highlights.",
                "anchors": ["dramatic_oil"],
                "model": "flux-2-pro",
                "score": 0.79,
                "tags": ["heroic", "oil", "tableau"],
                "notes": "Performs well on action-driven works.",
            },
            {
                "name": "Etching with Narrative Depth",
                "template": "Dense copperplate-style etching of a narrative turning point from {title}, emphasizing layered storytelling details.",
                "anchors": ["engraving_detailed", "warm_sepia_sketch"],
                "model": "gpt-image-1-medium",
                "score": 0.82,
                "tags": ["etching", "detail", "narrative"],
                "notes": "Reliable prompt for rich compositions with many micro-details.",
            },
            {
                "name": "Painterly Pastoral Variant",
                "template": "Pastoral yet classical rendering of a key emotional scene from {title}, favoring gentle brushwork over hard outlines.",
                "anchors": ["watercolor_soft", "dramatic_oil"],
                "model": "imagen-4-fast",
                "score": 0.74,
                "tags": ["pastoral", "painterly", "variant"],
                "notes": "Adds tonal diversity while staying aligned with the cover aesthetic.",
            },
        ]

        prompts = [
            LibraryPrompt(
                id=str(uuid.uuid4()),
                name=item["name"],
                prompt_template=item["template"],
                style_anchors=list(item["anchors"]),
                negative_prompt=negative_prompt,
                source_book="Moby Dick",
                source_model=item["model"],
                quality_score=float(item["score"]),
                saved_by="auto",
                created_at=_utc_now(),
                notes=item["notes"],
                tags=list(item["tags"]),
            )
            for item in starter_prompt_specs
        ]

        return anchors, prompts



def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _validate_prompt_template(template: str) -> None:
    if not _has_supported_placeholder(template):
        joined = ", ".join(SUPPORTED_REUSABLE_PLACEHOLDERS)
        raise ValueError(f"Prompt template must include at least one reusable placeholder ({joined}).")


def _has_supported_placeholder(template: str) -> bool:
    token = str(template or "")
    return any(marker in token for marker in SUPPORTED_REUSABLE_PLACEHOLDERS)


def _prompt_priority(prompt: LibraryPrompt) -> tuple[int, int, float, int, int, str]:
    tags = {str(tag).strip().lower() for tag in prompt.tags if str(tag).strip()}
    is_alexandria = 1 if "alexandria" in tags else 0
    is_builtin = 1 if str(prompt.category or "").strip().lower() == "builtin" else 0
    return (
        is_alexandria,
        is_builtin,
        float(prompt.quality_score or 0.0),
        int(prompt.win_count or 0),
        int(prompt.usage_count or 0),
        str(prompt.created_at or ""),
    )


def _sorted_prompts(prompts: Iterable[LibraryPrompt]) -> list[LibraryPrompt]:
    return sorted(prompts, key=_prompt_priority, reverse=True)


def load_default_prompt_library() -> PromptLibrary:
    """Load prompt library from default project location."""
    return PromptLibrary(config.PROMPT_LIBRARY_PATH)


def build_prompt_from_anchors(book_title: str, anchors: Iterable[str], custom_text: str = "") -> str:
    """Convenience wrapper for ad-hoc prompt creation."""
    library = load_default_prompt_library()
    return library.build_prompt(book_title=book_title, style_anchors=list(anchors), custom_text=custom_text)
