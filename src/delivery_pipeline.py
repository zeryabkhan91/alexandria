"""Automated delivery pipeline across export targets and Drive sync."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

try:
    from src import config
    from src import drive_manager
    from src import export_amazon
    from src import export_ingram
    from src import export_social
    from src import export_web
    from src import mockup_generator
    from src import safe_json
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import drive_manager  # type: ignore
    import export_amazon  # type: ignore
    import export_ingram  # type: ignore
    import export_social  # type: ignore
    import export_web  # type: ignore
    import mockup_generator  # type: ignore
    import safe_json  # type: ignore


ProgressCallback = Callable[[dict[str, Any]], None]
DEFAULT_PLATFORMS = ["amazon", "ingram", "social", "web", "gdrive"]
_VALID_PLATFORMS = set(DEFAULT_PLATFORMS)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path, default: Any) -> Any:
    return safe_json.load_json(path, default)


def _write_json(path: Path, payload: Any) -> None:
    safe_json.atomic_write_json(path, payload)


def _normalize_platforms(platforms: list[str] | None) -> list[str]:
    values = platforms if isinstance(platforms, list) and platforms else DEFAULT_PLATFORMS
    out: list[str] = []
    for raw in values:
        token = str(raw or "").strip().lower()
        if token and token in _VALID_PLATFORMS and token not in out:
            out.append(token)
    return out if out else DEFAULT_PLATFORMS.copy()


@dataclass(slots=True)
class DeliveryConfig:
    catalog_id: str
    enabled: bool
    auto_push_to_drive: bool
    platforms: list[str]
    updated_at: str


def _default_config(catalog_id: str) -> DeliveryConfig:
    return DeliveryConfig(
        catalog_id=str(catalog_id),
        enabled=False,
        auto_push_to_drive=True,
        platforms=DEFAULT_PLATFORMS.copy(),
        updated_at=_utc_now(),
    )


def get_config(*, catalog_id: str, config_path: Path) -> DeliveryConfig:
    payload = _load_json(config_path, {"catalogs": {}})
    catalogs = payload.get("catalogs", {}) if isinstance(payload, dict) else {}
    row = catalogs.get(catalog_id, {}) if isinstance(catalogs, dict) else {}
    if not isinstance(row, dict):
        return _default_config(catalog_id)
    return DeliveryConfig(
        catalog_id=str(catalog_id),
        enabled=bool(row.get("enabled", False)),
        auto_push_to_drive=bool(row.get("auto_push_to_drive", True)),
        platforms=_normalize_platforms(row.get("platforms")),
        updated_at=str(row.get("updated_at", _utc_now())),
    )


def set_enabled(*, catalog_id: str, enabled: bool, config_path: Path) -> DeliveryConfig:
    payload = _load_json(config_path, {"catalogs": {}})
    catalogs = payload.get("catalogs", {}) if isinstance(payload, dict) else {}
    if not isinstance(catalogs, dict):
        catalogs = {}
    current = get_config(catalog_id=catalog_id, config_path=config_path)
    current.enabled = bool(enabled)
    current.updated_at = _utc_now()
    catalogs[catalog_id] = {
        "enabled": current.enabled,
        "auto_push_to_drive": current.auto_push_to_drive,
        "platforms": current.platforms,
        "updated_at": current.updated_at,
    }
    payload = {"catalogs": catalogs, "updated_at": _utc_now()}
    _write_json(config_path, payload)
    return current


def _tracking_row(*, catalog_id: str, book_number: int, required_platforms: list[str] | None = None) -> dict[str, Any]:
    return {
        "catalog_id": str(catalog_id),
        "book_number": int(book_number),
        "fully_delivered": False,
        "required_platforms": _normalize_platforms(required_platforms),
        "updated_at": _utc_now(),
        "deliveries": {
            "amazon": {"status": "pending"},
            "ingram": {"status": "pending"},
            "social": {"status": "pending", "platforms_completed": 0, "platforms_total": 0},
            "web": {"status": "pending"},
            "gdrive": {"status": "pending"},
        },
    }


def _set_delivery_status(
    *,
    row: dict[str, Any],
    platform: str,
    status: str,
    file_count: int = 0,
    platforms_completed: int = 0,
    platforms_total: int = 0,
    error: str = "",
    required_platforms: list[str] | None = None,
) -> None:
    deliveries = row.get("deliveries", {})
    if not isinstance(deliveries, dict):
        deliveries = {}
    entry = deliveries.get(platform, {})
    if not isinstance(entry, dict):
        entry = {}
    entry["status"] = str(status)
    entry["timestamp"] = _utc_now()
    if file_count > 0:
        entry["file_count"] = int(file_count)
    if platforms_total > 0:
        entry["platforms_total"] = int(platforms_total)
        entry["platforms_completed"] = int(platforms_completed)
    if error:
        entry["error"] = str(error)
    deliveries[platform] = entry
    row["deliveries"] = deliveries
    row["updated_at"] = _utc_now()

    resolved_required = _normalize_platforms(required_platforms or row.get("required_platforms"))
    row["required_platforms"] = resolved_required
    done = True
    for token in resolved_required:
        status_token = str(deliveries.get(token, {}).get("status", "pending"))
        if status_token not in {"delivered", "skipped"}:
            done = False
            break
    row["fully_delivered"] = done


def get_tracking(*, catalog_id: str, tracking_path: Path) -> list[dict[str, Any]]:
    payload = _load_json(tracking_path, {"items": []})
    items = payload.get("items", []) if isinstance(payload, dict) else []
    out: list[dict[str, Any]] = []
    for row in items if isinstance(items, list) else []:
        if not isinstance(row, dict):
            continue
        if str(row.get("catalog_id", "")) != str(catalog_id):
            continue
        out.append(dict(row))
    out.sort(key=lambda row: int(row.get("book_number", 0)))
    return out


def _upsert_tracking_row(
    *,
    catalog_id: str,
    book_number: int,
    tracking_path: Path,
    required_platforms: list[str] | None = None,
) -> dict[str, Any]:
    payload = _load_json(tracking_path, {"items": []})
    items = payload.get("items", []) if isinstance(payload, dict) else []
    if not isinstance(items, list):
        items = []
    for row in items:
        if not isinstance(row, dict):
            continue
        if str(row.get("catalog_id")) == str(catalog_id) and int(row.get("book_number", 0)) == int(book_number):
            row["required_platforms"] = _normalize_platforms(required_platforms or row.get("required_platforms"))
            return row
    row = _tracking_row(catalog_id=catalog_id, book_number=book_number, required_platforms=required_platforms)
    items.append(row)
    payload = {"items": items, "updated_at": _utc_now()}
    _write_json(tracking_path, payload)
    return row


def _persist_tracking_row(*, row: dict[str, Any], tracking_path: Path) -> None:
    payload = _load_json(tracking_path, {"items": []})
    items = payload.get("items", []) if isinstance(payload, dict) else []
    if not isinstance(items, list):
        items = []
    replaced = False
    for idx, current in enumerate(items):
        if not isinstance(current, dict):
            continue
        if str(current.get("catalog_id")) == str(row.get("catalog_id")) and int(current.get("book_number", 0)) == int(row.get("book_number", 0)):
            items[idx] = row
            replaced = True
            break
    if not replaced:
        items.append(row)
    _write_json(tracking_path, {"items": items, "updated_at": _utc_now()})


def deliver_book(
    *,
    catalog_id: str,
    book_number: int,
    catalog_path: Path,
    output_root: Path,
    selections_path: Path,
    quality_path: Path,
    exports_root: Path,
    delivery_config_path: Path,
    delivery_tracking_path: Path,
    drive_folder_id: str,
    credentials_path: Path,
    progress_callback: ProgressCallback | None = None,
    platforms: list[str] | None = None,
) -> dict[str, Any]:
    cfg = get_config(catalog_id=catalog_id, config_path=delivery_config_path)
    active_platforms = _normalize_platforms(platforms or cfg.platforms)
    required_platforms = [token for token in active_platforms if not (token == "gdrive" and not cfg.auto_push_to_drive)]

    row = _upsert_tracking_row(
        catalog_id=catalog_id,
        book_number=book_number,
        tracking_path=delivery_tracking_path,
        required_platforms=required_platforms,
    )
    row["required_platforms"] = required_platforms

    def emit(stage: str, status: str, details: dict[str, Any] | None = None) -> None:
        if progress_callback:
            progress_callback(
                {
                    "catalog_id": catalog_id,
                    "book_number": int(book_number),
                    "stage": stage,
                    "status": status,
                    "timestamp": _utc_now(),
                    "details": details or {},
                }
            )

    emit("delivery", "started")
    result_payload: dict[str, Any] = {"book_number": int(book_number), "catalog_id": catalog_id, "steps": {}}

    if "amazon" in active_platforms:
        try:
            summary = export_amazon.export_book(
                book_number=book_number,
                catalog_id=catalog_id,
                catalog_path=catalog_path,
                output_root=output_root,
                selections_path=selections_path,
                quality_path=quality_path,
                exports_root=exports_root,
            )
            _set_delivery_status(
                row=row,
                platform="amazon",
                status="delivered",
                file_count=int(summary.get("file_count", 0)),
                required_platforms=required_platforms,
            )
            result_payload["steps"]["amazon"] = summary
            emit("amazon", "delivered", {"file_count": summary.get("file_count", 0)})
        except Exception as exc:
            _set_delivery_status(row=row, platform="amazon", status="failed", error=str(exc), required_platforms=required_platforms)
            result_payload["steps"]["amazon"] = {"error": str(exc)}
            emit("amazon", "failed", {"error": str(exc)})

    if "ingram" in active_platforms:
        try:
            summary = export_ingram.export_book(
                book_number=book_number,
                catalog_id=catalog_id,
                catalog_path=catalog_path,
                output_root=output_root,
                selections_path=selections_path,
                quality_path=quality_path,
                exports_root=exports_root,
            )
            _set_delivery_status(
                row=row,
                platform="ingram",
                status="delivered",
                file_count=int(summary.get("file_count", 0)),
                required_platforms=required_platforms,
            )
            result_payload["steps"]["ingram"] = summary
            emit("ingram", "delivered", {"file_count": summary.get("file_count", 0)})
        except Exception as exc:
            _set_delivery_status(row=row, platform="ingram", status="failed", error=str(exc), required_platforms=required_platforms)
            result_payload["steps"]["ingram"] = {"error": str(exc)}
            emit("ingram", "failed", {"error": str(exc)})

    if "social" in active_platforms:
        try:
            summary = export_social.export_book(
                book_number=book_number,
                catalog_id=catalog_id,
                catalog_path=catalog_path,
                output_root=output_root,
                selections_path=selections_path,
                quality_path=quality_path,
                exports_root=exports_root,
                platforms="all",
                watermark=True,
            )
            platforms_total = len(summary.get("platforms", []) or [])
            _set_delivery_status(
                row=row,
                platform="social",
                status="delivered",
                file_count=int(summary.get("file_count", 0)),
                platforms_completed=platforms_total,
                platforms_total=platforms_total,
                required_platforms=required_platforms,
            )
            result_payload["steps"]["social"] = summary
            emit("social", "delivered", {"file_count": summary.get("file_count", 0)})
        except Exception as exc:
            _set_delivery_status(row=row, platform="social", status="failed", error=str(exc), required_platforms=required_platforms)
            result_payload["steps"]["social"] = {"error": str(exc)}
            emit("social", "failed", {"error": str(exc)})

    if "web" in active_platforms:
        try:
            summary = export_web.export_book(
                book_number=book_number,
                catalog_id=catalog_id,
                catalog_path=catalog_path,
                output_root=output_root,
                selections_path=selections_path,
                quality_path=quality_path,
                exports_root=exports_root,
            )
            _set_delivery_status(
                row=row,
                platform="web",
                status="delivered",
                file_count=int(summary.get("file_count", 0)),
                required_platforms=required_platforms,
            )
            result_payload["steps"]["web"] = summary
            emit("web", "delivered", {"file_count": summary.get("file_count", 0)})
        except Exception as exc:
            _set_delivery_status(row=row, platform="web", status="failed", error=str(exc), required_platforms=required_platforms)
            result_payload["steps"]["web"] = {"error": str(exc)}
            emit("web", "failed", {"error": str(exc)})

    # Mockup generation step (required by delivery spec).
    try:
        mockup_summary = mockup_generator.generate_all_mockups(
            output_dir=str(output_root),
            selections_path=str(selections_path),
            books=[int(book_number)],
        )
        result_payload["steps"]["mockups"] = mockup_summary
        emit("mockups", "delivered", {"generated": mockup_summary.get("generated", 0)})
    except Exception as exc:
        result_payload["steps"]["mockups"] = {"error": str(exc)}
        emit("mockups", "failed", {"error": str(exc)})

    if "gdrive" in active_platforms and not cfg.auto_push_to_drive:
        _set_delivery_status(row=row, platform="gdrive", status="skipped", required_platforms=required_platforms)
        result_payload["steps"]["gdrive"] = {"status": "skipped", "reason": "auto_push_to_drive_disabled"}
        emit("gdrive", "skipped", {"reason": "auto_push_to_drive_disabled"})

    if "gdrive" in active_platforms and cfg.auto_push_to_drive:
        try:
            sync_state_path = config.gdrive_sync_state_path(catalog_id=catalog_id, data_dir=selections_path.parent)
            push_summary = drive_manager.push_to_drive(
                output_root=output_root,
                input_root=output_root.parent / "Input Covers",
                exports_root=exports_root,
                drive_folder_id=drive_folder_id,
                credentials_path=credentials_path,
                sync_state_path=sync_state_path,
                selected_relative_files=None,
            )
            if int(push_summary.get("failed", 0)) > 0:
                _set_delivery_status(
                    row=row,
                    platform="gdrive",
                    status="failed",
                    error="Drive push completed with failures",
                    required_platforms=required_platforms,
                )
            else:
                _set_delivery_status(
                    row=row,
                    platform="gdrive",
                    status="delivered",
                    file_count=int(push_summary.get("uploaded", 0)),
                    required_platforms=required_platforms,
                )
            result_payload["steps"]["gdrive"] = push_summary
            emit("gdrive", "delivered", {"uploaded": push_summary.get("uploaded", 0)})
        except Exception as exc:
            _set_delivery_status(row=row, platform="gdrive", status="failed", error=str(exc), required_platforms=required_platforms)
            result_payload["steps"]["gdrive"] = {"error": str(exc)}
            emit("gdrive", "failed", {"error": str(exc)})

    _persist_tracking_row(row=row, tracking_path=delivery_tracking_path)
    emit("delivery", "completed", {"fully_delivered": bool(row.get("fully_delivered", False))})
    result_payload["tracking"] = row
    return result_payload


def deliver_batch(
    *,
    catalog_id: str,
    book_numbers: list[int],
    catalog_path: Path,
    output_root: Path,
    selections_path: Path,
    quality_path: Path,
    exports_root: Path,
    delivery_config_path: Path,
    delivery_tracking_path: Path,
    drive_folder_id: str,
    credentials_path: Path,
    platforms: list[str] | None = None,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    total = len(book_numbers)
    for idx, book in enumerate(book_numbers, start=1):
        if progress_callback:
            progress_callback(
                {
                    "catalog_id": catalog_id,
                    "event": "job_progress",
                    "book_number": int(book),
                    "index": idx,
                    "total": total,
                    "progress": idx / max(1, total),
                    "timestamp": _utc_now(),
                }
            )
        try:
            results.append(
                deliver_book(
                    catalog_id=catalog_id,
                    book_number=int(book),
                    catalog_path=catalog_path,
                    output_root=output_root,
                    selections_path=selections_path,
                    quality_path=quality_path,
                    exports_root=exports_root,
                    delivery_config_path=delivery_config_path,
                    delivery_tracking_path=delivery_tracking_path,
                    drive_folder_id=drive_folder_id,
                    credentials_path=credentials_path,
                    platforms=platforms,
                    progress_callback=progress_callback,
                )
            )
        except Exception as exc:
            failures.append({"book_number": int(book), "error": str(exc)})
    return {
        "ok": len(failures) == 0,
        "catalog_id": catalog_id,
        "books_requested": int(total),
        "books_delivered": len(results),
        "results": results,
        "failures": failures,
        "generated_at": _utc_now(),
    }
