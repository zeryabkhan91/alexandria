from __future__ import annotations

import json
from pathlib import Path

from PIL import Image

from src import similarity_detector as sd
from tests.test_quality_review_server_smoke import _request_json, _start_server, _stop_server


def _save(path: Path, color=(120, 90, 60), size=(512, 512)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, color=color).save(path, format="JPEG")


def test_similarity_candidate_pairs_exhaustive_for_small_inputs():
    hashes = {
        idx: sd.CoverHashes(
            book_number=idx,
            image_path=f"{idx}.jpg",
            phash=f"{idx:016x}",
            dhash=f"{idx:016x}",
            color_hist=[0.0] * 96,
            dominant_colors=["#000000"],
        )
        for idx in range(1, 6)
    }
    pairs = sd._candidate_book_pairs(hashes=hashes, mode="auto")
    assert len(pairs) == 10


def test_similarity_candidate_pairs_lsh_for_large_inputs():
    hashes = {
        idx: sd.CoverHashes(
            book_number=idx,
            image_path=f"{idx}.jpg",
            phash=f"{(idx * 1315423911) & 0xFFFFFFFFFFFFFFFF:016x}",
            dhash=f"{(idx * 2654435761) & 0xFFFFFFFFFFFFFFFF:016x}",
            color_hist=[0.0] * 96,
            dominant_colors=["#000000"],
        )
        for idx in range(1, 701)
    }
    pairs = sd._candidate_book_pairs(hashes=hashes, mode="auto")
    exhaustive = (700 * 699) // 2
    assert len(pairs) < exhaustive
    assert len(pairs) > 0


def test_incremental_similarity_update_roundtrip(tmp_path: Path):
    output_dir = tmp_path / "Output Covers"
    catalog_path = tmp_path / "catalog.json"
    winners_path = tmp_path / "winners.json"
    regions_path = tmp_path / "regions.json"
    hashes_path = tmp_path / "hashes.json"
    matrix_path = tmp_path / "matrix.json"
    clusters_path = tmp_path / "clusters.json"

    _save(output_dir / "1. Book One" / "Variant-1" / "cover.jpg", color=(100, 120, 140))
    _save(output_dir / "2. Book Two" / "Variant-1" / "cover.jpg", color=(120, 120, 150))
    _save(output_dir / "3. Book Three" / "Variant-1" / "cover.jpg", color=(130, 120, 160))

    catalog_path.write_text(
        json.dumps(
            [
                {"number": 1, "folder_name": "1. Book One", "title": "Book One"},
                {"number": 2, "folder_name": "2. Book Two", "title": "Book Two"},
                {"number": 3, "folder_name": "3. Book Three", "title": "Book Three"},
            ]
        ),
        encoding="utf-8",
    )
    winners_path.write_text(
        json.dumps({"selections": {"1": {"winner": 1}, "2": {"winner": 1}, "3": {"winner": 1}}}),
        encoding="utf-8",
    )
    regions_path.write_text(
        json.dumps({"consensus_region": {"center_x": 256, "center_y": 256, "radius": 180}, "cover_size": {"width": 512, "height": 512}}),
        encoding="utf-8",
    )

    sd.run_similarity_analysis(
        output_dir=output_dir,
        threshold=0.9,
        catalog_path=catalog_path,
        winner_selections_path=winners_path,
        regions_path=regions_path,
        hashes_path=hashes_path,
        matrix_path=matrix_path,
        clusters_path=clusters_path,
        workers=1,
    )

    _save(output_dir / "2. Book Two" / "Variant-1" / "cover.jpg", color=(30, 40, 50))
    updated = sd.update_similarity_for_book(
        output_dir=output_dir,
        book_number=2,
        threshold=0.9,
        catalog_path=catalog_path,
        winner_selections_path=winners_path,
        regions_path=regions_path,
        hashes_path=hashes_path,
        matrix_path=matrix_path,
        clusters_path=clusters_path,
    )
    assert updated.get("ok") is True
    assert int(updated.get("compared_books", 0)) == 2

    matrix_payload = json.loads(matrix_path.read_text(encoding="utf-8"))
    assert int(matrix_payload.get("total_pairs", 0)) >= 2


def test_scale_endpoints_smoke():
    process, base_url = _start_server()
    try:
        for path in (
            "/api/similarity/recompute/status?catalog=classics",
            "/api/drive/sync-status?catalog=classics",
            "/api/export/status?catalog=classics&limit=5&offset=0",
        ):
            status, payload = _request_json(base_url, path)
            assert status == 200, path
            assert payload.get("ok") is True
    finally:
        _stop_server(process)


def test_similarity_recompute_trigger_endpoint_smoke():
    process, base_url = _start_server()
    try:
        status, payload = _request_json(
            base_url,
            "/api/similarity/recompute?catalog=classics",
            method="POST",
            payload={"threshold": 0.25, "reason": "test"},
        )
        assert status == 200
        assert payload.get("ok") is True
        assert "job" in payload
    finally:
        _stop_server(process)
