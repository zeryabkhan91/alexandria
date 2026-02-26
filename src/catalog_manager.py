"""Catalog registry management for multi-catalog workflows."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src import config
from src import safe_json


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slug(value: str) -> str:
    token = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return token or "catalog"


def _default_settings() -> dict[str, Any]:
    return {
        "default_provider": config.AI_PROVIDER,
        "default_model": config.AI_MODEL,
        "quality_threshold": config.MIN_QUALITY_SCORE,
        "variants_per_book": config.VARIANTS_PER_COVER,
        "prompt_library_file": str(config.PROMPT_LIBRARY_PATH),
    }


def _parse_folder(folder_name: str) -> tuple[int, str, str]:
    token = folder_name.strip()
    m = re.match(r"^(\d+)\.\s*(.+)$", token)
    if not m:
        raise ValueError("folder does not start with 'N. ' prefix")
    number = int(m.group(1))
    tail = m.group(2).strip()
    if " - " in tail:
        title, author = tail.rsplit(" - ", 1)
    elif " — " in tail:
        title, author = tail.rsplit(" — ", 1)
    else:
        title, author = tail, "Unknown"
    return number, title.strip(), author.strip()


@dataclass(slots=True)
class Catalog:
    catalog_id: str
    name: str
    description: str
    book_count: int
    created_at: str
    updated_at: str
    status: str
    settings: dict[str, Any]
    input_dir: str
    output_dir: str
    config_dir: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class CatalogManager:
    """CRUD management for config/catalogs.json and per-catalog metadata."""

    def __init__(self, *, catalogs_path: Path | None = None, project_root: Path | None = None):
        self.catalogs_path = catalogs_path or config.CATALOGS_PATH
        self.project_root = project_root or config.PROJECT_ROOT

    def _load(self) -> dict[str, Any]:
        payload = safe_json.load_json(self.catalogs_path, {})
        if not isinstance(payload, dict):
            payload = {}

        raw = payload.get("catalogs")
        catalogs: dict[str, dict[str, Any]] = {}
        if isinstance(raw, dict):
            for key, value in raw.items():
                if isinstance(value, dict):
                    entry = dict(value)
                    entry.setdefault("id", str(key))
                    catalogs[str(key)] = entry
        elif isinstance(raw, list):
            for row in raw:
                if not isinstance(row, dict):
                    continue
                cid = str(row.get("id", "")).strip()
                if not cid:
                    continue
                catalogs[cid] = dict(row)

        if not catalogs:
            fallback = config._default_catalog_payload().get("catalogs", [{}])[0]  # type: ignore[attr-defined]
            cid = str(fallback.get("id", "classics"))
            catalogs[cid] = dict(fallback)

        default_catalog = str(payload.get("default_catalog", "")).strip()
        if not default_catalog or default_catalog not in catalogs:
            default_catalog = next(iter(catalogs.keys()))

        normalized: dict[str, dict[str, Any]] = {}
        now = _utc_now_iso()
        for cid, row in catalogs.items():
            catalog_id = _slug(row.get("id", cid))
            normalized[catalog_id] = {
                "id": catalog_id,
                "name": str(row.get("name", catalog_id)),
                "description": str(row.get("description", "")),
                "book_count": int(row.get("book_count", 0) or 0),
                "created_at": str(row.get("created_at", now) or now),
                "updated_at": str(row.get("updated_at", now) or now),
                "status": str(row.get("status", "active") or "active"),
                "settings": row.get("settings", {}) if isinstance(row.get("settings"), dict) else {},
                "input_dir": str(row.get("input_dir", row.get("input_covers_dir", "Input Covers"))),
                "output_dir": str(row.get("output_dir", row.get("output_covers_dir", "Output Covers"))),
                "config_dir": str(row.get("config_dir", "config")),
                "catalog_file": str(row.get("catalog_file", f"config/book_catalog_{catalog_id}.json")),
                "prompts_file": str(row.get("prompts_file", f"config/book_prompts_{catalog_id}.json")),
                "input_covers_dir": str(row.get("input_covers_dir", row.get("input_dir", "Input Covers"))),
                "output_covers_dir": str(row.get("output_covers_dir", row.get("output_dir", "Output Covers"))),
                "cover_style": str(row.get("cover_style", "navy_gold_medallion")),
            }
            if catalog_id == "classics":
                normalized[catalog_id]["catalog_file"] = str(row.get("catalog_file", "config/book_catalog.json"))
                normalized[catalog_id]["prompts_file"] = str(row.get("prompts_file", "config/book_prompts.json"))
        return {
            "catalogs": normalized,
            "default_catalog": default_catalog if default_catalog in normalized else next(iter(normalized.keys())),
        }

    def _save(self, payload: dict[str, Any]) -> None:
        catalogs = payload.get("catalogs", {})
        if not isinstance(catalogs, dict):
            catalogs = {}
        out = {
            "catalogs": catalogs,
            "default_catalog": str(payload.get("default_catalog", "")),
        }
        self.catalogs_path.parent.mkdir(parents=True, exist_ok=True)
        safe_json.atomic_write_json(self.catalogs_path, out)

    def _catalog_to_dataclass(self, row: dict[str, Any]) -> Catalog:
        return Catalog(
            catalog_id=str(row.get("id", "")),
            name=str(row.get("name", "")),
            description=str(row.get("description", "")),
            book_count=int(row.get("book_count", 0) or 0),
            created_at=str(row.get("created_at", "")),
            updated_at=str(row.get("updated_at", "")),
            status=str(row.get("status", "active")),
            settings=row.get("settings", {}) if isinstance(row.get("settings"), dict) else {},
            input_dir=str(row.get("input_dir", "Input Covers")),
            output_dir=str(row.get("output_dir", "Output Covers")),
            config_dir=str(row.get("config_dir", "config")),
        )

    @staticmethod
    def _resolve_project_path(token: str, *, project_root: Path) -> Path:
        path = Path(token)
        if path.is_absolute():
            return path
        return project_root / path

    def _winner_path(self, catalog_id: str) -> Path:
        if catalog_id == "classics":
            return self.project_root / "data" / "winner_selections.json"
        return self.project_root / "data" / f"winner_selections_{catalog_id}.json"

    def list_catalogs(self) -> list[Catalog]:
        payload = self._load()
        rows = payload.get("catalogs", {})
        if not isinstance(rows, dict):
            rows = {}
        out = [self._catalog_to_dataclass(row) for row in rows.values() if isinstance(row, dict)]
        out.sort(key=lambda row: row.catalog_id)
        return out

    def get_catalog(self, catalog_id: str) -> Catalog:
        payload = self._load()
        rows = payload.get("catalogs", {})
        if not isinstance(rows, dict):
            rows = {}
        key = _slug(catalog_id)
        if key not in rows:
            raise KeyError(f"Catalog not found: {catalog_id}")
        return self._catalog_to_dataclass(rows[key])

    def get_default_catalog_id(self) -> str:
        payload = self._load()
        return str(payload.get("default_catalog", "classics"))

    def set_default_catalog(self, catalog_id: str) -> str:
        payload = self._load()
        rows = payload.get("catalogs", {})
        if not isinstance(rows, dict):
            rows = {}
        key = _slug(catalog_id)
        if key not in rows:
            raise KeyError(f"Catalog not found: {catalog_id}")
        payload["default_catalog"] = key
        self._save(payload)
        return key

    def _ensure_catalog_files(self, row: dict[str, Any]) -> None:
        cfg_dir = self._resolve_project_path(str(row.get("config_dir", "config")), project_root=self.project_root)
        cfg_dir.mkdir(parents=True, exist_ok=True)

        catalog_file = self._resolve_project_path(str(row.get("catalog_file", "")), project_root=self.project_root)
        prompts_file = self._resolve_project_path(str(row.get("prompts_file", "")), project_root=self.project_root)
        if not catalog_file.exists():
            safe_json.atomic_write_json(catalog_file, [])
        if not prompts_file.exists():
            safe_json.atomic_write_json(prompts_file, {"books": []})

    def create_catalog(
        self,
        *,
        name: str,
        description: str = "",
        input_dir: str = "Input Covers",
        output_dir: str = "Output Covers",
        config_dir: str = "config",
        catalog_id: str | None = None,
    ) -> Catalog:
        payload = self._load()
        rows = payload.get("catalogs", {})
        if not isinstance(rows, dict):
            rows = {}

        cid = _slug(catalog_id or name)
        if cid in rows:
            raise ValueError(f"Catalog already exists: {cid}")

        now = _utc_now_iso()
        row = {
            "id": cid,
            "name": str(name or cid),
            "description": str(description or ""),
            "book_count": 0,
            "created_at": now,
            "updated_at": now,
            "status": "draft",
            "settings": _default_settings(),
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "config_dir": str(config_dir),
            "catalog_file": f"config/book_catalog_{cid}.json",
            "prompts_file": f"config/book_prompts_{cid}.json",
            "input_covers_dir": str(input_dir),
            "output_covers_dir": str(output_dir),
            "cover_style": "navy_gold_medallion",
        }
        self._ensure_catalog_files(row)
        rows[cid] = row
        payload["catalogs"] = rows
        if not str(payload.get("default_catalog", "")).strip():
            payload["default_catalog"] = cid
        self._save(payload)
        return self._catalog_to_dataclass(row)

    def update_catalog(self, catalog_id: str, updates: dict[str, Any]) -> Catalog:
        payload = self._load()
        rows = payload.get("catalogs", {})
        if not isinstance(rows, dict):
            rows = {}
        cid = _slug(catalog_id)
        row = rows.get(cid)
        if not isinstance(row, dict):
            raise KeyError(f"Catalog not found: {catalog_id}")

        for field in ["name", "description", "status", "input_dir", "output_dir", "config_dir"]:
            if field in updates:
                row[field] = updates[field]
        if "settings" in updates and isinstance(updates.get("settings"), dict):
            merged = dict(row.get("settings", {}))
            merged.update(updates.get("settings", {}))
            row["settings"] = merged

        row["input_covers_dir"] = str(row.get("input_dir", row.get("input_covers_dir", "Input Covers")))
        row["output_covers_dir"] = str(row.get("output_dir", row.get("output_covers_dir", "Output Covers")))
        row["updated_at"] = _utc_now_iso()
        rows[cid] = row
        payload["catalogs"] = rows
        self._save(payload)
        return self._catalog_to_dataclass(row)

    def archive_catalog(self, catalog_id: str) -> Catalog:
        return self.update_catalog(catalog_id, {"status": "archived"})

    def activate_catalog(self, catalog_id: str) -> Catalog:
        return self.update_catalog(catalog_id, {"status": "active"})

    def clone_catalog(self, catalog_id: str, *, new_id: str | None = None, name: str | None = None) -> Catalog:
        source = self.get_catalog(catalog_id)
        clone_id = _slug(new_id or f"{source.catalog_id}-clone")
        payload = self._load()
        rows = payload.get("catalogs", {})
        if not isinstance(rows, dict):
            rows = {}
        if clone_id in rows:
            raise ValueError(f"Catalog already exists: {clone_id}")
        now = _utc_now_iso()
        row = {
            "id": clone_id,
            "name": str(name or f"{source.name} Clone"),
            "description": str(source.description),
            "book_count": 0,
            "created_at": now,
            "updated_at": now,
            "status": "draft",
            "settings": dict(source.settings),
            "input_dir": source.input_dir,
            "output_dir": source.output_dir,
            "config_dir": source.config_dir,
            "catalog_file": f"config/book_catalog_{clone_id}.json",
            "prompts_file": f"config/book_prompts_{clone_id}.json",
            "input_covers_dir": source.input_dir,
            "output_covers_dir": source.output_dir,
            "cover_style": "navy_gold_medallion",
        }
        self._ensure_catalog_files(row)
        rows[clone_id] = row
        payload["catalogs"] = rows
        self._save(payload)
        return self._catalog_to_dataclass(row)

    def get_settings(self, catalog_id: str) -> dict[str, Any]:
        row = self.get_catalog(catalog_id)
        defaults = _default_settings()
        merged = dict(defaults)
        merged.update(row.settings or {})
        return merged

    def update_settings(self, catalog_id: str, settings: dict[str, Any]) -> dict[str, Any]:
        current = self.get_settings(catalog_id)
        current.update(settings or {})
        self.update_catalog(catalog_id, {"settings": current})
        return current

    def _read_catalog_books(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        catalog_file = self._resolve_project_path(str(row.get("catalog_file", "")), project_root=self.project_root)
        payload = safe_json.load_json(catalog_file, [])
        return payload if isinstance(payload, list) else []

    def _write_catalog_books(self, row: dict[str, Any], books: list[dict[str, Any]]) -> None:
        catalog_file = self._resolve_project_path(str(row.get("catalog_file", "")), project_root=self.project_root)
        safe_json.atomic_write_json(catalog_file, books)

    def import_books(self, catalog_id: str, *, source_dir: str | None = None) -> dict[str, Any]:
        payload = self._load()
        rows = payload.get("catalogs", {})
        if not isinstance(rows, dict):
            rows = {}
        cid = _slug(catalog_id)
        row = rows.get(cid)
        if not isinstance(row, dict):
            raise KeyError(f"Catalog not found: {catalog_id}")

        input_dir = self._resolve_project_path(
            str(source_dir or row.get("input_dir", row.get("input_covers_dir", "Input Covers"))),
            project_root=self.project_root,
        )
        if not input_dir.exists() or not input_dir.is_dir():
            raise FileNotFoundError(f"Input directory not found: {input_dir}")

        existing = self._read_catalog_books(row)
        existing_numbers = {int(book.get("number", 0)) for book in existing if isinstance(book, dict)}
        existing_folders = {str(book.get("folder_name", "")) for book in existing if isinstance(book, dict)}

        discovered: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for folder in sorted([item for item in input_dir.iterdir() if item.is_dir()], key=lambda p: p.name.lower()):
            try:
                number, title, author = _parse_folder(folder.name)
            except ValueError:
                continue
            row_payload = {
                "number": number,
                "title": title,
                "author": author,
                "folder_name": folder.name,
                "file_base": f"{title} - {author}",
                "genre": "unknown",
                "themes": [],
            }
            if number in existing_numbers or folder.name in existing_folders:
                skipped.append(row_payload)
            else:
                discovered.append(row_payload)

        merged = sorted(existing + discovered, key=lambda entry: int(entry.get("number", 0)))
        self._write_catalog_books(row, merged)
        row["book_count"] = len(merged)
        row["updated_at"] = _utc_now_iso()
        rows[cid] = row
        payload["catalogs"] = rows
        self._save(payload)
        return {
            "catalog_id": cid,
            "input_dir": str(input_dir),
            "found_total": len(discovered) + len(skipped),
            "imported": len(discovered),
            "skipped": len(skipped),
            "imported_books": discovered,
            "skipped_books": skipped,
            "book_count": len(merged),
        }

    def stats_for_catalog(self, catalog_id: str) -> dict[str, Any]:
        catalog = self.get_catalog(catalog_id)
        row = self._load().get("catalogs", {}).get(catalog.catalog_id, {})
        books = self._read_catalog_books(row if isinstance(row, dict) else {})
        total_books = len(books)

        output_dir = self._resolve_project_path(catalog.output_dir, project_root=self.project_root)
        processed_books = 0
        if output_dir.exists():
            for book in books:
                folder = str(book.get("folder_name", ""))
                if folder.endswith(" copy"):
                    folder = folder[:-5]
                if not folder:
                    continue
                if (output_dir / folder).exists():
                    processed_books += 1

        winner_path = self._winner_path(catalog.catalog_id)
        winners_payload = safe_json.load_json(winner_path, {"selections": {}})
        selections = winners_payload.get("selections", winners_payload) if isinstance(winners_payload, dict) else {}
        winner_count = len(selections) if isinstance(selections, dict) else 0

        return {
            "catalog_id": catalog.catalog_id,
            "book_count": total_books or catalog.book_count,
            "processed_count": processed_books,
            "winner_count": winner_count,
            "processed_percent": round((processed_books / max(1, total_books)) * 100.0, 2) if total_books else 0.0,
            "last_activity": catalog.updated_at,
        }

    def export_catalog_bundle(self, catalog_id: str) -> dict[str, Any]:
        payload = self._load()
        rows = payload.get("catalogs", {})
        if not isinstance(rows, dict):
            rows = {}
        cid = _slug(catalog_id)
        row = rows.get(cid)
        if not isinstance(row, dict):
            raise KeyError(f"Catalog not found: {catalog_id}")
        books = self._read_catalog_books(row)
        winner_path = self._winner_path(cid)
        winner_payload = safe_json.load_json(winner_path, {"selections": {}})
        return {
            "exported_at": _utc_now_iso(),
            "catalog": row,
            "books": books,
            "winner_selections": winner_payload,
            "stats": self.stats_for_catalog(cid),
        }

