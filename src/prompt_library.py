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


class PromptLibrary:
    """Manages style anchors and saved prompts for single-cover iteration and bulk runs."""

    def __init__(self, library_path: Path):
        self.library_path = library_path
        self.library_path.parent.mkdir(parents=True, exist_ok=True)
        self._style_anchors: dict[str, StyleAnchor] = {}
        self._prompts: dict[str, LibraryPrompt] = {}
        self._load_or_seed()

    def get_style_anchors(self) -> list[StyleAnchor]:
        """Return all available style anchors."""
        return sorted(self._style_anchors.values(), key=lambda anchor: anchor.name)

    def save_prompt(self, prompt: LibraryPrompt) -> None:
        """Save a successful prompt to the library."""
        if "{title}" not in prompt.prompt_template:
            raise ValueError("Prompt template must include '{title}' placeholder for title-agnostic reuse.")
        self._prompts[prompt.id] = prompt
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
        return sorted(values, key=lambda prompt: prompt.quality_score, reverse=True)

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

        return sorted((prompt for prompt in self._prompts.values() if _matches(prompt)), key=lambda p: p.quality_score, reverse=True)

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
        if self.library_path.exists():
            self._load()
            if self._style_anchors and self._prompts:
                return

        anchors, prompts = self._seed_library()
        self._style_anchors = {anchor.name: anchor for anchor in anchors}
        self._prompts = {prompt.id: prompt for prompt in prompts}
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
            )
        self._prompts = prompts

    def _persist(self) -> None:
        payload = {
            "version": 1,
            "updated_at": _utc_now(),
            "style_anchors": [asdict(anchor) for anchor in self.get_style_anchors()],
            "prompts": [
                asdict(prompt)
                for prompt in sorted(
                    self._prompts.values(),
                    key=lambda item: (item.quality_score, item.created_at),
                    reverse=True,
                )
            ],
        }
        safe_json.atomic_write_json(self.library_path, payload)
        logger.debug("Persisted prompt library", extra={"path": str(self.library_path), "anchors": len(self._style_anchors), "prompts": len(self._prompts)})

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


def load_default_prompt_library() -> PromptLibrary:
    """Load prompt library from default project location."""
    return PromptLibrary(config.PROMPT_LIBRARY_PATH)


def build_prompt_from_anchors(book_title: str, anchors: Iterable[str], custom_text: str = "") -> str:
    """Convenience wrapper for ad-hoc prompt creation."""
    library = load_default_prompt_library()
    return library.build_prompt(book_title=book_title, style_anchors=list(anchors), custom_text=custom_text)
