from __future__ import annotations

from pathlib import Path

from PIL import Image

from src.thumbnail_server import ThumbnailServer


def _make_image(path: Path, size: tuple[int, int] = (1200, 900)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    img = Image.new("RGB", size, color=(50, 90, 150))
    img.save(path, format="JPEG")


def test_thumbnail_generation_creates_file(tmp_path: Path):
    project_root = tmp_path / "project"
    source = project_root / "Output Covers" / "book1" / "variant_1.jpg"
    _make_image(source)

    server = ThumbnailServer(project_root=project_root, cache_dir=project_root / "tmp" / "thumbs")
    rel = str(source.relative_to(project_root))

    thumb = server.thumbnail_for(relative_path=rel, size="small")
    assert thumb is not None
    assert thumb.exists()

    with Image.open(thumb) as img:
        assert max(img.size) <= 200


def test_thumbnail_generation_reuses_existing(tmp_path: Path):
    project_root = tmp_path / "project"
    source = project_root / "Output Covers" / "book1" / "variant_1.jpg"
    _make_image(source)

    server = ThumbnailServer(project_root=project_root, cache_dir=project_root / "tmp" / "thumbs")
    rel = str(source.relative_to(project_root))

    first = server.thumbnail_for(relative_path=rel, size="medium")
    second = server.thumbnail_for(relative_path=rel, size="medium")
    assert first is not None and second is not None
    assert first == second


def test_thumbnail_rejects_invalid_size(tmp_path: Path):
    project_root = tmp_path / "project"
    source = project_root / "x.jpg"
    _make_image(source)
    server = ThumbnailServer(project_root=project_root, cache_dir=project_root / "tmp" / "thumbs")
    rel = str(source.relative_to(project_root))
    assert server.thumbnail_for(relative_path=rel, size="huge") is None


def test_thumbnail_rejects_path_outside_project(tmp_path: Path):
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "outside.jpg"
    _make_image(outside)
    server = ThumbnailServer(project_root=project_root, cache_dir=project_root / "tmp" / "thumbs")
    assert server.thumbnail_for(relative_path=str(outside), size="small") is None


def test_thumbnail_missing_source_returns_none(tmp_path: Path):
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True, exist_ok=True)
    server = ThumbnailServer(project_root=project_root, cache_dir=project_root / "tmp" / "thumbs")
    assert server.thumbnail_for(relative_path="missing.jpg", size="small") is None


def test_thumbnail_rejects_non_image_source(tmp_path: Path):
    project_root = tmp_path / "project"
    source = project_root / "Output Covers" / "book1" / "variant_1.txt"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("not an image", encoding="utf-8")

    server = ThumbnailServer(project_root=project_root, cache_dir=project_root / "tmp" / "thumbs")
    rel = str(source.relative_to(project_root))
    assert server.thumbnail_for(relative_path=rel, size="small") is None


def test_thumbnail_rejects_source_outside_allowed_roots(tmp_path: Path):
    project_root = tmp_path / "project"
    source = project_root / "config" / "secret.jpg"
    _make_image(source)
    allowed_root = project_root / "Output Covers"
    allowed_root.mkdir(parents=True, exist_ok=True)

    server = ThumbnailServer(
        project_root=project_root,
        cache_dir=project_root / "tmp" / "thumbs",
        allowed_roots=[allowed_root],
    )
    rel = str(source.relative_to(project_root))
    assert server.thumbnail_for(relative_path=rel, size="small") is None


def test_thumbnail_rejects_invalid_path_tokens(tmp_path: Path):
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True, exist_ok=True)
    server = ThumbnailServer(project_root=project_root, cache_dir=project_root / "tmp" / "thumbs")

    assert server.thumbnail_for(relative_path="", size="small") is None
    assert server.thumbnail_for(relative_path="bad\x00name.jpg", size="small") is None
    assert server.thumbnail_for(relative_path="../escape.jpg", size="small") is None


def test_thumbnail_rejects_symlink_escape(tmp_path: Path):
    project_root = tmp_path / "project"
    outside_root = tmp_path / "outside"
    outside_root.mkdir(parents=True, exist_ok=True)
    source = outside_root / "cover.jpg"
    _make_image(source)

    project_root.mkdir(parents=True, exist_ok=True)
    link = project_root / "linked"
    link.symlink_to(outside_root, target_is_directory=True)

    server = ThumbnailServer(project_root=project_root, cache_dir=project_root / "tmp" / "thumbs")
    assert server.thumbnail_for(relative_path="linked/cover.jpg", size="small") is None
