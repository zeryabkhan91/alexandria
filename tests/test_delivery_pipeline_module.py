from __future__ import annotations

import json
from pathlib import Path

from src import delivery_pipeline


def _runtime_fixture(tmp_path: Path) -> dict[str, Path]:
    catalog_path = tmp_path / "book_catalog.json"
    output_root = tmp_path / "Output Covers"
    selections_path = tmp_path / "winner_selections.json"
    quality_path = tmp_path / "quality_scores.json"
    exports_root = tmp_path / "exports"
    delivery_config_path = tmp_path / "delivery_pipeline.json"
    delivery_tracking_path = tmp_path / "delivery_tracking.json"
    credentials_path = tmp_path / "credentials.json"
    credentials_path.write_text("{}", encoding="utf-8")

    catalog_path.write_text(json.dumps([{"number": 1, "title": "A", "author": "B", "folder_name": "1. A - B"}]), encoding="utf-8")
    output_root.mkdir(parents=True, exist_ok=True)
    selections_path.write_text(json.dumps({"selections": {"1": {"winner": 1}}}), encoding="utf-8")
    quality_path.write_text(json.dumps({"scores": []}), encoding="utf-8")
    return {
        "catalog_path": catalog_path,
        "output_root": output_root,
        "selections_path": selections_path,
        "quality_path": quality_path,
        "exports_root": exports_root,
        "delivery_config_path": delivery_config_path,
        "delivery_tracking_path": delivery_tracking_path,
        "credentials_path": credentials_path,
    }


def test_delivery_enable_and_tracking_flow(tmp_path: Path, monkeypatch):
    fx = _runtime_fixture(tmp_path)
    cfg = delivery_pipeline.set_enabled(catalog_id="classics", enabled=True, config_path=fx["delivery_config_path"])
    assert cfg.enabled is True

    monkeypatch.setattr(
        delivery_pipeline.export_amazon,
        "export_book",
        lambda **_kwargs: {"file_count": 7, "ok": True},
    )
    monkeypatch.setattr(
        delivery_pipeline.export_ingram,
        "export_book",
        lambda **_kwargs: {"file_count": 1, "ok": True},
    )
    monkeypatch.setattr(
        delivery_pipeline.export_social,
        "export_book",
        lambda **_kwargs: {"file_count": 5, "platforms": ["instagram", "facebook"], "ok": True},
    )
    monkeypatch.setattr(
        delivery_pipeline.export_web,
        "export_book",
        lambda **_kwargs: {"file_count": 6, "ok": True},
    )
    monkeypatch.setattr(
        delivery_pipeline.mockup_generator,
        "generate_all_mockups",
        lambda **_kwargs: {"generated": 3},
    )
    monkeypatch.setattr(
        delivery_pipeline.drive_manager,
        "push_to_drive",
        lambda **_kwargs: {"uploaded": 2, "failed": 0},
    )

    events: list[dict] = []
    result = delivery_pipeline.deliver_book(
        catalog_id="classics",
        book_number=1,
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
        delivery_config_path=fx["delivery_config_path"],
        delivery_tracking_path=fx["delivery_tracking_path"],
        drive_folder_id="local:/tmp/mirror",
        credentials_path=fx["credentials_path"],
        progress_callback=events.append,
    )
    assert result["tracking"]["fully_delivered"] is True
    assert events

    tracking = delivery_pipeline.get_tracking(catalog_id="classics", tracking_path=fx["delivery_tracking_path"])
    assert len(tracking) == 1
    assert tracking[0]["fully_delivered"] is True


def test_delivery_batch_handles_failures(tmp_path: Path, monkeypatch):
    fx = _runtime_fixture(tmp_path)
    delivery_pipeline.set_enabled(catalog_id="classics", enabled=True, config_path=fx["delivery_config_path"])

    def _fake_deliver_book(**kwargs):  # type: ignore[no-untyped-def]
        if int(kwargs["book_number"]) == 2:
            raise RuntimeError("boom")
        return {"tracking": {"book_number": kwargs["book_number"]}}

    monkeypatch.setattr(delivery_pipeline, "deliver_book", _fake_deliver_book)

    summary = delivery_pipeline.deliver_batch(
        catalog_id="classics",
        book_numbers=[1, 2],
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
        delivery_config_path=fx["delivery_config_path"],
        delivery_tracking_path=fx["delivery_tracking_path"],
        drive_folder_id="local:/tmp/mirror",
        credentials_path=fx["credentials_path"],
    )
    assert summary["books_delivered"] == 1
    assert len(summary["failures"]) == 1


def test_delivery_subset_platforms_marks_fully_delivered(tmp_path: Path, monkeypatch):
    fx = _runtime_fixture(tmp_path)
    delivery_pipeline.set_enabled(catalog_id="classics", enabled=True, config_path=fx["delivery_config_path"])

    monkeypatch.setattr(
        delivery_pipeline.export_amazon,
        "export_book",
        lambda **_kwargs: {"file_count": 7, "ok": True},
    )
    monkeypatch.setattr(delivery_pipeline.export_ingram, "export_book", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("unexpected")))
    monkeypatch.setattr(delivery_pipeline.export_social, "export_book", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("unexpected")))
    monkeypatch.setattr(delivery_pipeline.export_web, "export_book", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("unexpected")))
    monkeypatch.setattr(
        delivery_pipeline.mockup_generator,
        "generate_all_mockups",
        lambda **_kwargs: {"generated": 1},
    )
    monkeypatch.setattr(delivery_pipeline.drive_manager, "push_to_drive", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("unexpected")))

    result = delivery_pipeline.deliver_book(
        catalog_id="classics",
        book_number=1,
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
        delivery_config_path=fx["delivery_config_path"],
        delivery_tracking_path=fx["delivery_tracking_path"],
        drive_folder_id="local:/tmp/mirror",
        credentials_path=fx["credentials_path"],
        platforms=["amazon"],
    )
    assert result["tracking"]["fully_delivered"] is True
    assert result["tracking"]["required_platforms"] == ["amazon"]
    assert result["tracking"]["deliveries"]["amazon"]["status"] == "delivered"
    assert result["tracking"]["deliveries"]["gdrive"]["status"] == "pending"


def test_delivery_gdrive_skipped_when_auto_push_disabled(tmp_path: Path, monkeypatch):
    fx = _runtime_fixture(tmp_path)
    delivery_pipeline.set_enabled(catalog_id="classics", enabled=True, config_path=fx["delivery_config_path"])
    payload = json.loads(fx["delivery_config_path"].read_text(encoding="utf-8"))
    payload["catalogs"]["classics"]["auto_push_to_drive"] = False
    fx["delivery_config_path"].write_text(json.dumps(payload, indent=2), encoding="utf-8")

    monkeypatch.setattr(
        delivery_pipeline.export_amazon,
        "export_book",
        lambda **_kwargs: {"file_count": 7, "ok": True},
    )
    monkeypatch.setattr(
        delivery_pipeline.mockup_generator,
        "generate_all_mockups",
        lambda **_kwargs: {"generated": 1},
    )
    monkeypatch.setattr(
        delivery_pipeline.drive_manager,
        "push_to_drive",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("should not be called")),
    )

    result = delivery_pipeline.deliver_book(
        catalog_id="classics",
        book_number=1,
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
        delivery_config_path=fx["delivery_config_path"],
        delivery_tracking_path=fx["delivery_tracking_path"],
        drive_folder_id="local:/tmp/mirror",
        credentials_path=fx["credentials_path"],
        platforms=["amazon", "gdrive"],
    )
    assert result["tracking"]["fully_delivered"] is True
    assert result["tracking"]["deliveries"]["amazon"]["status"] == "delivered"
    assert result["tracking"]["deliveries"]["gdrive"]["status"] == "skipped"
    assert result["steps"]["gdrive"]["status"] == "skipped"


def test_delivery_gdrive_uses_catalog_scoped_sync_state_path(tmp_path: Path, monkeypatch):
    fx = _runtime_fixture(tmp_path)
    delivery_pipeline.set_enabled(catalog_id="demo", enabled=True, config_path=fx["delivery_config_path"])

    monkeypatch.setattr(delivery_pipeline.export_amazon, "export_book", lambda **_kwargs: {"file_count": 1, "ok": True})
    monkeypatch.setattr(delivery_pipeline.export_ingram, "export_book", lambda **_kwargs: {"file_count": 1, "ok": True})
    monkeypatch.setattr(delivery_pipeline.export_social, "export_book", lambda **_kwargs: {"file_count": 1, "platforms": ["instagram"], "ok": True})
    monkeypatch.setattr(delivery_pipeline.export_web, "export_book", lambda **_kwargs: {"file_count": 1, "ok": True})
    monkeypatch.setattr(delivery_pipeline.mockup_generator, "generate_all_mockups", lambda **_kwargs: {"generated": 1})

    captured: dict[str, object] = {}

    def _fake_push_to_drive(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return {"uploaded": 1, "failed": 0}

    monkeypatch.setattr(delivery_pipeline.drive_manager, "push_to_drive", _fake_push_to_drive)

    result = delivery_pipeline.deliver_book(
        catalog_id="demo",
        book_number=1,
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
        delivery_config_path=fx["delivery_config_path"],
        delivery_tracking_path=fx["delivery_tracking_path"],
        drive_folder_id="remote-folder-id",
        credentials_path=fx["credentials_path"],
    )

    expected = delivery_pipeline.config.gdrive_sync_state_path(catalog_id="demo", data_dir=fx["selections_path"].parent)
    assert captured.get("sync_state_path") == expected
    assert result["tracking"]["deliveries"]["gdrive"]["status"] == "delivered"


def test_delivery_helper_paths_handle_invalid_payload_shapes(tmp_path: Path):
    config_path = tmp_path / "delivery.json"
    config_path.write_text("{invalid", encoding="utf-8")
    cfg = delivery_pipeline.get_config(catalog_id="classics", config_path=config_path)
    assert cfg.catalog_id == "classics"
    assert cfg.platforms == delivery_pipeline.DEFAULT_PLATFORMS

    config_path.write_text(json.dumps([]), encoding="utf-8")
    cfg2 = delivery_pipeline.set_enabled(catalog_id="classics", enabled=True, config_path=config_path)
    assert cfg2.enabled is True

    tracking_path = tmp_path / "tracking.json"
    tracking_path.write_text(
        json.dumps({"items": ["bad-row", {"catalog_id": "other", "book_number": 1}], "updated_at": "now"}),
        encoding="utf-8",
    )
    assert delivery_pipeline.get_tracking(catalog_id="classics", tracking_path=tracking_path) == []

    row = {"deliveries": [], "required_platforms": ["amazon"]}
    delivery_pipeline._set_delivery_status(row=row, platform="amazon", status="failed", error="boom")
    assert row["deliveries"]["amazon"]["error"] == "boom"
    assert row["fully_delivered"] is False

    tracking_path.write_text(json.dumps({"items": "bad"}), encoding="utf-8")
    upserted = delivery_pipeline._upsert_tracking_row(
        catalog_id="classics",
        book_number=7,
        tracking_path=tracking_path,
        required_platforms=["amazon"],
    )
    assert upserted["book_number"] == 7

    existing = {"catalog_id": "classics", "book_number": 7, "deliveries": {}, "required_platforms": ["web"]}
    tracking_path.write_text(json.dumps({"items": [existing]}), encoding="utf-8")
    upserted_existing = delivery_pipeline._upsert_tracking_row(
        catalog_id="classics",
        book_number=7,
        tracking_path=tracking_path,
        required_platforms=["amazon"],
    )
    assert upserted_existing["required_platforms"] == ["amazon"]

    tracking_path.write_text(json.dumps({"items": "bad"}), encoding="utf-8")
    delivery_pipeline._persist_tracking_row(row={"catalog_id": "classics", "book_number": 99, "deliveries": {}}, tracking_path=tracking_path)
    persisted = json.loads(tracking_path.read_text(encoding="utf-8"))
    assert len(persisted["items"]) == 1

    tracking_path.write_text(
        json.dumps(
            {
                "items": ["bad-row", {"catalog_id": "classics", "book_number": 99, "deliveries": {"amazon": {"status": "pending"}}}],
            }
        ),
        encoding="utf-8",
    )
    delivery_pipeline._persist_tracking_row(row={"catalog_id": "classics", "book_number": 99, "deliveries": {"amazon": {"status": "delivered"}}}, tracking_path=tracking_path)
    persisted2 = json.loads(tracking_path.read_text(encoding="utf-8"))
    assert any(isinstance(item, dict) and item.get("book_number") == 99 for item in persisted2["items"])


def test_delivery_json_helpers_use_safe_json(tmp_path: Path, monkeypatch):
    captured: dict[str, object] = {}

    def _fake_load(path, default):  # type: ignore[no-untyped-def]
        captured["load"] = (path, default)
        return {"hello": "world"}

    def _fake_write(path, payload):  # type: ignore[no-untyped-def]
        captured["write"] = (path, payload)

    monkeypatch.setattr(delivery_pipeline.safe_json, "load_json", _fake_load)
    monkeypatch.setattr(delivery_pipeline.safe_json, "atomic_write_json", _fake_write)

    source = tmp_path / "input.json"
    loaded = delivery_pipeline._load_json(source, {"default": True})
    delivery_pipeline._write_json(source, {"ok": True})

    assert loaded == {"hello": "world"}
    assert captured["load"] == (source, {"default": True})
    assert captured["write"] == (source, {"ok": True})


def test_delivery_book_platform_failure_paths_and_gdrive_partial_failure(tmp_path: Path, monkeypatch):
    fx = _runtime_fixture(tmp_path)
    delivery_pipeline.set_enabled(catalog_id="classics", enabled=True, config_path=fx["delivery_config_path"])

    monkeypatch.setattr(delivery_pipeline.export_amazon, "export_book", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("amazon-fail")))
    monkeypatch.setattr(delivery_pipeline.export_ingram, "export_book", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("ingram-fail")))
    monkeypatch.setattr(delivery_pipeline.export_social, "export_book", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("social-fail")))
    monkeypatch.setattr(delivery_pipeline.export_web, "export_book", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("web-fail")))
    monkeypatch.setattr(delivery_pipeline.mockup_generator, "generate_all_mockups", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("mockup-fail")))
    monkeypatch.setattr(delivery_pipeline.drive_manager, "push_to_drive", lambda **_kwargs: {"uploaded": 0, "failed": 2})

    result = delivery_pipeline.deliver_book(
        catalog_id="classics",
        book_number=1,
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
        delivery_config_path=fx["delivery_config_path"],
        delivery_tracking_path=fx["delivery_tracking_path"],
        drive_folder_id="local:/tmp/mirror",
        credentials_path=fx["credentials_path"],
    )
    assert "error" in result["steps"]["amazon"]
    assert "error" in result["steps"]["ingram"]
    assert "error" in result["steps"]["social"]
    assert "error" in result["steps"]["web"]
    assert "error" in result["steps"]["mockups"]
    assert result["tracking"]["deliveries"]["gdrive"]["status"] == "failed"
    assert result["tracking"]["fully_delivered"] is False


def test_delivery_book_gdrive_exception_path(tmp_path: Path, monkeypatch):
    fx = _runtime_fixture(tmp_path)
    delivery_pipeline.set_enabled(catalog_id="classics", enabled=True, config_path=fx["delivery_config_path"])
    monkeypatch.setattr(delivery_pipeline.mockup_generator, "generate_all_mockups", lambda **_kwargs: {"generated": 0})
    monkeypatch.setattr(
        delivery_pipeline.drive_manager,
        "push_to_drive",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("drive-down")),
    )
    result = delivery_pipeline.deliver_book(
        catalog_id="classics",
        book_number=1,
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
        delivery_config_path=fx["delivery_config_path"],
        delivery_tracking_path=fx["delivery_tracking_path"],
        drive_folder_id="local:/tmp/mirror",
        credentials_path=fx["credentials_path"],
        platforms=["gdrive"],
    )
    assert result["tracking"]["deliveries"]["gdrive"]["status"] == "failed"
    assert "error" in result["steps"]["gdrive"]


def test_delivery_batch_progress_callback_reports_progress(tmp_path: Path, monkeypatch):
    fx = _runtime_fixture(tmp_path)
    delivery_pipeline.set_enabled(catalog_id="classics", enabled=True, config_path=fx["delivery_config_path"])
    monkeypatch.setattr(delivery_pipeline, "deliver_book", lambda **kwargs: {"tracking": {"book_number": kwargs["book_number"]}})

    events: list[dict] = []
    summary = delivery_pipeline.deliver_batch(
        catalog_id="classics",
        book_numbers=[1, 2, 3],
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
        delivery_config_path=fx["delivery_config_path"],
        delivery_tracking_path=fx["delivery_tracking_path"],
        drive_folder_id="local:/tmp/mirror",
        credentials_path=fx["credentials_path"],
        progress_callback=events.append,
    )
    assert summary["books_delivered"] == 3
    assert len(events) == 3
    assert all(event["event"] == "job_progress" for event in events)
