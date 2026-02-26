from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
from PIL import Image

from src import similarity_detector as sd


def _save(path: Path, color=(120, 90, 60), size=(512, 512)):
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, color=color).save(path, format="JPEG")


def test_prompt_text_similarity_and_tokenizers():
    assert sd.prompt_text_similarity("a b c", "a b c") > 0.99
    assert sd.prompt_text_similarity("a b c", "x y z") < 0.2
    assert sd._tokenize_text("Hello, World!") == ["hello", "world"]


def test_bits_and_distance_helpers():
    bits = np.array([1, 0, 1, 1, 0, 0, 1, 0], dtype=np.uint8)
    token = sd._bits_to_hex(bits)
    recovered = sd._hex_to_bits(token)
    assert len(recovered) >= len(bits.tolist())
    assert sd._hamming_distance([1, 0, 1], [1, 1, 0]) == 2
    assert sd._chi_squared_distance(np.array([0.5, 0.5]), np.array([0.5, 0.5])) == 0.0


def test_safe_helpers_and_parse_books():
    assert sd._safe_int("5", 0) == 5
    assert sd._safe_int("bad", 7) == 7
    assert sd._safe_float("0.4", 0.0) == 0.4
    assert sd._safe_float("bad", 0.5) == 0.5
    assert sd._parse_books("1,3-4") == [1, 3, 4]


def test_extract_medallion_and_hash_building(tmp_path: Path):
    path = tmp_path / "cover.jpg"
    _save(path, size=(3784, 2777))
    regions = {"consensus_region": {"center_x": 2864, "center_y": 1620, "radius": 500}, "cover_size": {"width": 3784, "height": 2777}}
    with Image.open(path) as img:
        med = sd._extract_medallion_region(img.convert("RGB"), regions)
    assert med.size[0] == med.size[1]
    assert med.size[0] > 0

    hashes = sd._compute_hash_for_book(book_number=1, image_path=path, regions=regions)
    assert hashes.book_number == 1
    assert len(hashes.phash) > 0
    assert len(hashes.dhash) > 0
    assert len(hashes.color_hist) > 0


def test_compare_hash_objects_and_alert_reason():
    a = sd.CoverHashes(book_number=1, image_path="a.jpg", phash="ffff", dhash="0000", color_hist=[0.5, 0.5], dominant_colors=["#112233"])
    b = sd.CoverHashes(book_number=2, image_path="b.jpg", phash="ffff", dhash="0000", color_hist=[0.5, 0.5], dominant_colors=["#112233"])
    metrics = sd._compare_hash_objects(a, b)
    assert 0.0 <= metrics["combined_similarity"] <= 1.0
    reason = sd._build_alert_reason(a, b, metrics)
    assert isinstance(reason, str) and len(reason) > 0


def test_matrix_and_cluster_detection():
    hashes = {
        1: sd.CoverHashes(1, "a", "ffff", "ffff", [0.5, 0.5], ["#111111"]),
        2: sd.CoverHashes(2, "b", "ffff", "ffff", [0.5, 0.5], ["#111111"]),
        3: sd.CoverHashes(3, "c", "0000", "0000", [1.0, 0.0], ["#ffffff"]),
    }
    matrix = sd.build_similarity_matrix(hashes=hashes, threshold=0.5)
    assert matrix["total_pairs"] == 3
    clusters = sd.detect_clusters(pairs=matrix["pairs"], threshold=0.5)
    assert "clusters" in clusters


def test_cover_hash_row_and_json_loaders(tmp_path: Path):
    row = sd._cover_hash_from_row(book=1, value={"phash": "aa", "dhash": "bb", "color_hist": [0.1, 0.9], "dominant_colors": ["#000000"]})
    assert row is not None
    assert row.book_number == 1

    dict_path = tmp_path / "dict.json"
    list_path = tmp_path / "list.json"
    dict_path.write_text(json.dumps({"x": 1}), encoding="utf-8")
    list_path.write_text(json.dumps([{"n": 1}]), encoding="utf-8")
    assert sd._load_json_dict(dict_path)["x"] == 1
    assert sd._load_json_list(list_path)[0]["n"] == 1
    assert sd._load_region_config(tmp_path / "missing.json")["consensus_region"]["radius"] > 0


def test_winner_cover_paths_and_dismissed_pairs(tmp_path: Path):
    output_dir = tmp_path / "Output Covers"
    catalog_path = tmp_path / "catalog.json"
    selections_path = tmp_path / "winners.json"
    dismissed_path = tmp_path / "dismissed.json"

    (output_dir / "1. Book One" / "Variant-1").mkdir(parents=True, exist_ok=True)
    _save(output_dir / "1. Book One" / "Variant-1" / "cover.jpg")
    catalog_path.write_text(json.dumps([{"number": 1, "folder_name": "1. Book One"}]), encoding="utf-8")
    selections_path.write_text(json.dumps({"selections": {"1": {"winner": 1}}}), encoding="utf-8")

    winners = sd._load_winner_cover_paths(output_dir=output_dir, catalog_path=catalog_path, winner_selections_path=selections_path)
    assert 1 in winners
    assert winners[1].exists()

    out = sd.dismiss_similarity_pair(book_a=1, book_b=3, dismissed_path=dismissed_path)
    assert "pairs" in out
    pairs = sd.load_dismissed_pairs(dismissed_path)
    assert "1-3" in pairs


def test_check_prompt_similarity_against_winners(tmp_path: Path):
    winners = tmp_path / "winners.json"
    history = tmp_path / "history.json"
    winners.write_text(json.dumps({"selections": {"2": {"winner": 1}}}), encoding="utf-8")
    history.write_text(
        json.dumps(
            {
                "items": [
                    {"book_number": 2, "variant": 1, "prompt": "storm sea whale captain ahab"},
                    {"book_number": 3, "variant": 1, "prompt": "gentle tea party garden"},
                ]
            }
        ),
        encoding="utf-8",
    )
    result = sd.check_prompt_similarity_against_winners(
        prompt="storm sea whale captain ahab",
        current_book=1,
        winner_selections_path=winners,
        generation_history_path=history,
        threshold=0.8,
    )
    assert result["closest_book"] == 2
    assert result["alert"] is True


def test_compute_cover_hashes_parallel(tmp_path: Path):
    winners = {}
    for idx, color in [(1, (100, 120, 150)), (2, (120, 120, 150))]:
        p = tmp_path / f"{idx}.jpg"
        _save(p, color=color)
        winners[idx] = p
    regions = {"consensus_region": {"center_x": 256, "center_y": 256, "radius": 180}, "cover_size": {"width": 512, "height": 512}}
    hashes = sd.compute_cover_hashes(winners=winners, regions=regions, workers=2)
    assert set(hashes.keys()) == {1, 2}


def test_load_or_build_winner_hashes_cache_and_rebuild(tmp_path: Path, monkeypatch):
    winners = {}
    for idx in [1, 2]:
        p = tmp_path / f"{idx}.jpg"
        _save(p)
        winners[idx] = p
    regions = {"consensus_region": {"center_x": 256, "center_y": 256, "radius": 180}, "cover_size": {"width": 512, "height": 512}}
    hashes_path = tmp_path / "hashes.json"

    # Rebuild path.
    rebuilt = sd._load_or_build_winner_hashes(winners=winners, regions=regions, hashes_path=hashes_path)
    assert set(rebuilt.keys()) == {1, 2}
    assert hashes_path.exists()

    # Cached path.
    cached = sd._load_or_build_winner_hashes(winners=winners, regions=regions, hashes_path=hashes_path)
    assert set(cached.keys()) == {1, 2}


def test_check_generated_image_against_winners(tmp_path: Path):
    output_dir = tmp_path / "Output Covers"
    catalog_path = tmp_path / "catalog.json"
    winners_path = tmp_path / "winners.json"
    regions_path = tmp_path / "regions.json"

    # Winner covers
    _save(output_dir / "1. Book One" / "Variant-1" / "cover.jpg", color=(100, 120, 140))
    _save(output_dir / "2. Book Two" / "Variant-1" / "cover.jpg", color=(101, 121, 141))
    probe = tmp_path / "probe.jpg"
    _save(probe, color=(102, 122, 142))

    catalog_path.write_text(
        json.dumps(
            [
                {"number": 1, "folder_name": "1. Book One"},
                {"number": 2, "folder_name": "2. Book Two"},
                {"number": 3, "folder_name": "3. Book Three"},
            ]
        ),
        encoding="utf-8",
    )
    winners_path.write_text(json.dumps({"selections": {"1": {"winner": 1}, "2": {"winner": 1}}}), encoding="utf-8")
    regions_path.write_text(json.dumps({"consensus_region": {"center_x": 256, "center_y": 256, "radius": 180}, "cover_size": {"width": 512, "height": 512}}), encoding="utf-8")

    result = sd.check_generated_image_against_winners(
        image_path=probe,
        book_number=3,
        output_dir=output_dir,
        catalog_path=catalog_path,
        winner_selections_path=winners_path,
        regions_path=regions_path,
        threshold=0.95,
    )
    assert result["closest_book"] in {1, 2}
    assert "metrics" in result


def test_run_similarity_analysis_and_report(tmp_path: Path):
    output_dir = tmp_path / "Output Covers"
    catalog_path = tmp_path / "catalog.json"
    winners_path = tmp_path / "winners.json"
    regions_path = tmp_path / "regions.json"
    hashes_path = tmp_path / "hashes.json"
    matrix_path = tmp_path / "matrix.json"
    clusters_path = tmp_path / "clusters.json"
    report_path = tmp_path / "report.html"

    _save(output_dir / "1. Book One" / "Variant-1" / "cover.jpg", color=(110, 120, 130))
    _save(output_dir / "2. Book Two" / "Variant-1" / "cover.jpg", color=(210, 120, 130))
    catalog_path.write_text(json.dumps([{"number": 1, "folder_name": "1. Book One", "title": "Book One"}, {"number": 2, "folder_name": "2. Book Two", "title": "Book Two"}]), encoding="utf-8")
    winners_path.write_text(json.dumps({"selections": {"1": {"winner": 1}, "2": {"winner": 1}}}), encoding="utf-8")
    regions_path.write_text(json.dumps({"consensus_region": {"center_x": 256, "center_y": 256, "radius": 180}, "cover_size": {"width": 512, "height": 512}}), encoding="utf-8")

    summary = sd.run_similarity_analysis(
        output_dir=output_dir,
        threshold=0.95,
        catalog_path=catalog_path,
        winner_selections_path=winners_path,
        regions_path=regions_path,
        hashes_path=hashes_path,
        matrix_path=matrix_path,
        clusters_path=clusters_path,
        workers=1,
    )
    assert summary["books"] == 2
    assert matrix_path.exists()

    report = sd.generate_report_html(
        matrix_path=matrix_path,
        output_path=report_path,
        output_dir=output_dir,
        catalog_path=catalog_path,
        winner_selections_path=winners_path,
        threshold=0.95,
    )
    assert report.exists()
    assert "Similarity Alert Report" in report.read_text(encoding="utf-8")


def test_run_similarity_analysis_uses_atomic_many_write(tmp_path: Path, monkeypatch):
    hashes_path = tmp_path / "hashes.json"
    matrix_path = tmp_path / "matrix.json"
    clusters_path = tmp_path / "clusters.json"

    monkeypatch.setattr(sd, "_load_winner_cover_paths", lambda **_kwargs: {})
    monkeypatch.setattr(sd, "_load_region_config", lambda _path: {})
    monkeypatch.setattr(sd, "compute_cover_hashes", lambda **_kwargs: {})
    monkeypatch.setattr(
        sd,
        "build_similarity_matrix",
        lambda **_kwargs: {"pairs": [], "total_pairs": 0, "alerts": 0},
    )
    monkeypatch.setattr(sd, "detect_clusters", lambda **_kwargs: {"clusters": []})

    captured: dict[str, object] = {}

    def _capture(items):  # type: ignore[no-untyped-def]
        captured["items"] = items

    monkeypatch.setattr(sd.safe_json, "atomic_write_many_json", _capture)

    summary = sd.run_similarity_analysis(
        output_dir=tmp_path / "Output Covers",
        catalog_path=tmp_path / "catalog.json",
        winner_selections_path=tmp_path / "winners.json",
        regions_path=tmp_path / "regions.json",
        hashes_path=hashes_path,
        matrix_path=matrix_path,
        clusters_path=clusters_path,
    )
    assert summary["books"] == 0
    items = captured["items"]
    assert isinstance(items, list)
    assert [row[0] for row in items] == [hashes_path, matrix_path, clusters_path]


def test_similarity_writers_use_atomic_json(tmp_path: Path, monkeypatch):
    captured: list[tuple[Path, dict]] = []

    def _capture(path, payload):  # type: ignore[no-untyped-def]
        captured.append((path, payload))

    monkeypatch.setattr(sd.safe_json, "atomic_write_json", _capture)
    monkeypatch.setattr(sd, "compute_cover_hashes", lambda **_kwargs: {})
    monkeypatch.setattr(sd, "_load_json_dict", lambda _path: {})

    hashes_path = tmp_path / "hashes.json"
    sd._load_or_build_winner_hashes(winners={}, regions={}, hashes_path=hashes_path)
    sd.dismiss_similarity_pair(book_a=1, book_b=2, dismissed_path=tmp_path / "dismissed.json")

    assert captured
    assert any(path.name == "dismissed.json" for path, _payload in captured)


def test_similarity_loader_and_parse_edge_cases(tmp_path: Path):
    assert sd._parse_books(None) is None
    assert sd._parse_books("0,bad,3-1") == [1, 2, 3]

    dict_like = tmp_path / "dict_like.json"
    list_like = tmp_path / "list_like.json"
    dict_like.write_text(json.dumps([{"x": 1}]), encoding="utf-8")
    list_like.write_text(json.dumps({"x": 1}), encoding="utf-8")
    assert sd._load_json_dict(dict_like) == {}
    assert sd._load_json_list(list_like) == []

    assert sd._find_first_jpg(tmp_path / "missing") is None

    invalid = sd._cover_hash_from_row(book=1, value={"phash": "", "dhash": ""})
    assert invalid is None

    fallback = sd._cover_hash_from_row(
        book=3,
        value={
            "phash": "aa",
            "dhash": "bb",
            "color_hist": "bad",
            "dominant_colors": "",
        },
    )
    assert fallback is not None
    assert len(fallback.color_hist) == 96
    assert fallback.dominant_colors == ["#000000"]

    outside = sd._relative_or_str(tmp_path / "a.jpg", tmp_path / "different-root")
    assert outside.endswith("a.jpg")


def test_winner_cover_paths_fallback_mode(tmp_path: Path):
    output_dir = tmp_path / "Output Covers"
    catalog_path = tmp_path / "catalog.json"
    winners_path = tmp_path / "winners.json"

    _save(output_dir / "1. Book One" / "Variant-1" / "cover.jpg")
    _save(output_dir / "2. Book Two" / "Variant-1" / "cover.jpg")

    catalog_path.write_text(
        json.dumps(
            [
                {"number": 0, "folder_name": "ignored"},
                {"number": 1, "folder_name": "1. Book One copy"},
                {"number": 2, "folder_name": "2. Book Two"},
            ]
        ),
        encoding="utf-8",
    )
    winners_path.write_text("{bad-json", encoding="utf-8")

    winners = sd._load_winner_cover_paths(
        output_dir=output_dir,
        catalog_path=catalog_path,
        winner_selections_path=winners_path,
    )
    assert set(winners.keys()) == {1, 2}


def test_similarity_main_paths(tmp_path: Path, monkeypatch):
    runtime = SimpleNamespace(
        book_catalog_path=tmp_path / "catalog.json",
        data_dir=tmp_path / "data",
        config_dir=tmp_path / "config",
    )
    runtime.data_dir.mkdir(parents=True, exist_ok=True)
    runtime.config_dir.mkdir(parents=True, exist_ok=True)

    args = SimpleNamespace(
        output_dir=tmp_path / "output",
        threshold=0.5,
        book=1,
        report=True,
        output=tmp_path / "report.html",
        books="1-2",
    )

    monkeypatch.setattr(sd.argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(sd.config, "get_config", lambda: runtime)
    monkeypatch.setattr(sd, "run_similarity_analysis", lambda **_kwargs: {"books": 1})
    monkeypatch.setattr(sd, "_load_winner_cover_paths", lambda **_kwargs: {1: tmp_path / "winner.jpg"})
    monkeypatch.setattr(sd, "check_generated_image_against_winners", lambda **_kwargs: {"closest_book": 2})
    monkeypatch.setattr(sd, "generate_report_html", lambda **_kwargs: args.output)

    assert sd.main() == 0


def test_similarity_main_missing_book_image(tmp_path: Path, monkeypatch):
    runtime = SimpleNamespace(
        book_catalog_path=tmp_path / "catalog.json",
        data_dir=tmp_path / "data",
        config_dir=tmp_path / "config",
    )
    runtime.data_dir.mkdir(parents=True, exist_ok=True)
    runtime.config_dir.mkdir(parents=True, exist_ok=True)

    args = SimpleNamespace(
        output_dir=tmp_path / "output",
        threshold=0.5,
        book=9,
        report=False,
        output=tmp_path / "report.html",
        books=None,
    )

    monkeypatch.setattr(sd.argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(sd.config, "get_config", lambda: runtime)
    monkeypatch.setattr(sd, "run_similarity_analysis", lambda **_kwargs: {"books": 0})
    monkeypatch.setattr(sd, "_load_winner_cover_paths", lambda **_kwargs: {})

    assert sd.main() == 0
