"""On-demand thumbnail generation and lookup."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable

from PIL import Image


class ThumbnailServer:
    SIZES = {
        "small": 200,
        "medium": 400,
        "large": 800,
    }

    def __init__(self, *, project_root: Path, cache_dir: Path, allowed_roots: Iterable[Path] | None = None):
        self.project_root = project_root.resolve()
        self.cache_dir = cache_dir.resolve()
        roots = list(allowed_roots or [self.project_root])
        self.allowed_roots = [Path(root).resolve() for root in roots]
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _is_allowed_source(self, source: Path) -> bool:
        for root in self.allowed_roots:
            try:
                source.relative_to(root)
                return True
            except ValueError:
                continue
        return False

    def _resolve_source(self, relative_path: str) -> Path | None:
        token = str(relative_path or "").strip().lstrip("/")
        if not token:
            return None
        if "\x00" in token:
            return None
        if ".." in token.replace("\\", "/"):
            return None
        source = (self.project_root / token).resolve()
        try:
            source.relative_to(self.project_root)
        except ValueError:
            return None
        if not self._is_allowed_source(source):
            return None
        if not source.exists() or not source.is_file():
            return None
        return source

    def thumbnail_for(self, *, relative_path: str, size: str) -> Path | None:
        source = self._resolve_source(relative_path)
        if source is None:
            return None
        if size not in self.SIZES:
            return None

        rel = source.relative_to(self.project_root)
        digest = hashlib.sha1(str(rel).encode("utf-8"), usedforsecurity=False).hexdigest()[:16]
        target = self.cache_dir / size / rel.parent / f"{rel.stem}-{digest}.jpg"
        if target.exists():
            return target

        max_dim = self.SIZES[size]
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            with Image.open(source) as img:
                rgb = img.convert("RGB")
                rgb.thumbnail((max_dim, max_dim), Image.LANCZOS)
                rgb.save(target, format="JPEG", quality=82, optimize=True)
        except Exception:
            # Non-image/corrupt sources should fail closed without bubbling to API handlers.
            try:
                if target.exists():
                    target.unlink()
            except Exception:
                pass
            return None
        return target
