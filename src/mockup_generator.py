"""Prompt 11D mockup generation engine for product, lifestyle, and marketplace assets."""

from __future__ import annotations

import argparse
import io
import json
import math
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont
try:
    import cv2  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    cv2 = None  # type: ignore

try:
    from src import config
    from src import image_generator
    from src import safe_json
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import image_generator  # type: ignore
    import safe_json  # type: ignore
    from logger import get_logger  # type: ignore

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_CONFIG_PATH = config.CONFIG_DIR / "mockup_templates.json"
BACKGROUND_PROMPTS_PATH = config.CONFIG_DIR / "mockup_background_prompts.json"
TEMPLATE_ASSETS_DIR = config.CONFIG_DIR / "mockup_templates"
CUSTOM_BACKGROUNDS_DIR = TEMPLATE_ASSETS_DIR / "custom"

DEFAULT_OUTPUT_ROOT = config.OUTPUT_DIR
DEFAULT_MOCKUPS_ROOT = DEFAULT_OUTPUT_ROOT / "Mockups"
DEFAULT_AMAZON_ROOT = DEFAULT_OUTPUT_ROOT / "Amazon"
DEFAULT_SOCIAL_ROOT = DEFAULT_OUTPUT_ROOT / "Social"

LIFESTYLE_TEMPLATES = {"desk_scene", "bookshelf", "reading_chair", "window_light", "library_table"}


@dataclass(slots=True)
class MockupTemplate:
    id: str
    name: str
    category: str
    description: str
    base_image: Path
    mask_image: Path
    transform: dict[str, Any]
    output_size: tuple[int, int]
    use_case: str


@dataclass(slots=True)
class BookRecord:
    number: int
    title: str
    author: str
    folder_name: str


class MockupGenerationError(RuntimeError):
    """Raised for mockup-specific failures."""


def load_templates(path: Path = TEMPLATE_CONFIG_PATH) -> list[MockupTemplate]:
    payload = safe_json.load_json(path, {})
    rows = payload.get("templates", []) if isinstance(payload, dict) else []
    templates: list[MockupTemplate] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        output = row.get("output_size", [1200, 1200])
        width = int(output[0]) if isinstance(output, list) and len(output) >= 2 else 1200
        height = int(output[1]) if isinstance(output, list) and len(output) >= 2 else 1200
        width = max(320, width)
        height = max(320, height)
        longest = max(width, height)
        if longest < 1200:
            scale = 1200 / float(longest)
            width = int(round(width * scale))
            height = int(round(height * scale))
        templates.append(
            MockupTemplate(
                id=str(row.get("id", "")).strip(),
                name=str(row.get("name", "")).strip(),
                category=str(row.get("category", "product")).strip(),
                description=str(row.get("description", "")).strip(),
                base_image=_resolve_path(str(row.get("base_image", ""))),
                mask_image=_resolve_path(str(row.get("mask_image", ""))),
                transform=row.get("transform", {}) if isinstance(row.get("transform"), dict) else {},
                output_size=(width, height),
                use_case=str(row.get("use_case", "")).strip(),
            )
        )
    return [template for template in templates if template.id]


def template_map(path: Path = TEMPLATE_CONFIG_PATH) -> dict[str, MockupTemplate]:
    return {template.id: template for template in load_templates(path)}


def load_background_prompts(path: Path = BACKGROUND_PROMPTS_PATH) -> dict[str, dict[str, str]]:
    payload = safe_json.load_json(path, {})
    out: dict[str, dict[str, str]] = {}
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(value, dict):
                out[str(key)] = {
                    "prompt": str(value.get("prompt", "")),
                    "negative": str(value.get("negative", "")),
                    "size": str(value.get("size", "1600x1200")),
                }
    return out


def _resolve_path(token: str) -> Path:
    candidate = Path(token)
    if candidate.is_absolute():
        return candidate
    return PROJECT_ROOT / candidate


def _normalise_folder_name(name: str) -> str:
    folder = str(name or "").strip()
    if folder.endswith(" copy"):
        return folder[:-5]
    return folder


def load_book_records(catalog_path: Path | None = None) -> dict[int, BookRecord]:
    runtime = config.get_config()
    payload = safe_json.load_json((catalog_path or runtime.book_catalog_path), [])
    records: dict[int, BookRecord] = {}
    if not isinstance(payload, list):
        return records
    for row in payload:
        if not isinstance(row, dict):
            continue
        number = _safe_int(row.get("number"), 0)
        if number <= 0:
            continue
        records[number] = BookRecord(
            number=number,
            title=str(row.get("title", "Book")),
            author=str(row.get("author", "Unknown")),
            folder_name=_normalise_folder_name(str(row.get("folder_name", f"{number}. Book"))),
        )
    return records


def load_winner_map(selections_path: Path) -> dict[int, int]:
    payload = safe_json.load_json(selections_path, {})
    if isinstance(payload, dict) and isinstance(payload.get("selections"), dict):
        selections = payload.get("selections", {})
    elif isinstance(payload, dict):
        selections = payload
    else:
        selections = {}

    out: dict[int, int] = {}
    for key, value in selections.items():
        book = _safe_int(key, 0)
        if book <= 0:
            continue
        if isinstance(value, dict):
            variant = _safe_int(value.get("winner"), 0)
        else:
            variant = _safe_int(value, 0)
        if variant > 0:
            out[book] = variant
    return out


def winner_cover_path(*, book_number: int, output_root: Path, catalog: dict[int, BookRecord], winner_map: dict[int, int]) -> Path:
    record = catalog.get(book_number)
    if not record:
        raise MockupGenerationError(f"Book {book_number} not found in catalog")
    winner = winner_map.get(book_number)
    if not winner:
        raise MockupGenerationError(f"Book {book_number} has no winner selection")

    variant_dir = output_root / record.folder_name / f"Variant-{winner}"
    images = sorted(variant_dir.glob("*.jpg"))
    if not images:
        raise MockupGenerationError(f"Winner JPG missing for book {book_number} at {variant_dir}")
    return images[0]


def ensure_template_assets(*, templates: list[MockupTemplate], force: bool = False, generate_backgrounds: bool = False) -> dict[str, Any]:
    TEMPLATE_ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    CUSTOM_BACKGROUNDS_DIR.mkdir(parents=True, exist_ok=True)

    background_prompts = load_background_prompts()
    runtime = config.get_config()
    created_bases = 0
    created_masks = 0

    for template in templates:
        template.base_image.parent.mkdir(parents=True, exist_ok=True)
        template.mask_image.parent.mkdir(parents=True, exist_ok=True)

        if force or not template.base_image.exists():
            if template.id in LIFESTYLE_TEMPLATES and generate_backgrounds:
                prompt_cfg = background_prompts.get(template.id, {})
                image = _generate_background_scene(template, prompt_cfg=prompt_cfg, runtime=runtime)
            else:
                image = _build_base_scene(template)
            image.save(template.base_image, format="PNG")
            created_bases += 1

        if force or not template.mask_image.exists():
            mask = _build_mask_image(template)
            mask.save(template.mask_image, format="PNG")
            created_masks += 1

    return {
        "templates": len(templates),
        "bases_created": created_bases,
        "masks_created": created_masks,
        "assets_dir": str(TEMPLATE_ASSETS_DIR),
    }


def _build_base_scene(template: MockupTemplate) -> Image.Image:
    width, height = template.output_size
    canvas = Image.new("RGBA", (width, height), (245, 245, 245, 255))
    draw = ImageDraw.Draw(canvas, "RGBA")

    if template.category == "product":
        draw.rectangle((0, 0, width, height), fill=(246, 247, 250, 255))
        for idx in range(0, height, max(36, height // 25)):
            alpha = 20 if idx % 2 == 0 else 12
            draw.line((0, idx, width, idx), fill=(210, 216, 228, alpha), width=2)
        draw.ellipse(
            (int(width * 0.28), int(height * 0.70), int(width * 0.78), int(height * 0.95)),
            fill=(0, 0, 0, 26),
        )
    elif template.category == "lifestyle":
        _draw_lifestyle_fallback(draw, width=width, height=height, template_id=template.id)
    elif template.id == "kindle_tablet":
        draw.rectangle((0, 0, width, height), fill=(234, 237, 244, 255))
        device = [int(width * 0.22), int(height * 0.08), int(width * 0.82), int(height * 0.94)]
        draw.rounded_rectangle(device, radius=48, fill=(30, 35, 46, 255), outline=(10, 12, 16, 255), width=4)
        screen = [int(width * 0.30), int(height * 0.17), int(width * 0.74), int(height * 0.86)]
        draw.rounded_rectangle(screen, radius=16, fill=(248, 248, 252, 255))
        draw.ellipse(
            (int(width * 0.49), int(height * 0.90), int(width * 0.53), int(height * 0.94)),
            fill=(80, 86, 102, 255),
        )
    elif template.id == "social_card":
        draw.rectangle((0, 0, width, height), fill=(26, 39, 68, 255))
        draw.rectangle((0, 0, int(width * 0.4), height), fill=(21, 32, 58, 255))
        draw.polygon(
            [(int(width * 0.38), 0), (int(width * 0.56), 0), (int(width * 0.34), height)],
            fill=(196, 163, 82, 55),
        )
        draw.line((int(width * 0.03), int(height * 0.94), int(width * 0.97), int(height * 0.94)), fill=(196, 163, 82, 200), width=4)
    else:
        draw.rectangle((0, 0, width, height), fill=(241, 244, 250, 255))

    return canvas


def _draw_lifestyle_fallback(draw: ImageDraw.ImageDraw, *, width: int, height: int, template_id: str) -> None:
    if template_id == "desk_scene":
        draw.rectangle((0, 0, width, height), fill=(76, 54, 40, 255))
        draw.rectangle((0, int(height * 0.48), width, height), fill=(92, 63, 45, 255))
        draw.ellipse((60, int(height * 0.62), 300, int(height * 0.92)), fill=(205, 201, 186, 220))
        draw.ellipse((90, int(height * 0.66), 270, int(height * 0.88)), fill=(108, 72, 52, 245))
    elif template_id == "bookshelf":
        draw.rectangle((0, 0, width, height), fill=(92, 66, 42, 255))
        for idx in range(8):
            x = int(width * 0.07) + idx * int(width * 0.11)
            draw.rounded_rectangle((x, 40, x + 92, int(height * 0.86)), radius=8, fill=(120 - idx * 5, 84 + idx * 3, 58 + idx * 2, 255))
        draw.rectangle((0, int(height * 0.86), width, height), fill=(70, 48, 30, 255))
    elif template_id == "reading_chair":
        draw.rectangle((0, 0, width, height), fill=(96, 74, 58, 255))
        draw.rounded_rectangle((int(width * 0.05), int(height * 0.26), int(width * 0.94), int(height * 0.98)), radius=80, fill=(114, 83, 61, 255))
        draw.ellipse((int(width * 0.65), 40, int(width * 0.95), int(height * 0.36)), fill=(252, 236, 188, 92))
    elif template_id == "window_light":
        draw.rectangle((0, 0, width, height), fill=(202, 188, 163, 255))
        draw.rectangle((0, int(height * 0.30), width, height), fill=(188, 165, 132, 255))
        draw.rectangle((int(width * 0.08), 0, int(width * 0.92), int(height * 0.62)), fill=(245, 236, 220, 170))
        draw.line((int(width * 0.5), 0, int(width * 0.5), int(height * 0.62)), fill=(230, 215, 183, 220), width=6)
    else:  # library_table
        draw.rectangle((0, 0, width, height), fill=(34, 56, 46, 255))
        draw.rectangle((0, int(height * 0.45), width, height), fill=(26, 72, 55, 255))
        draw.rectangle((40, int(height * 0.52), width - 40, int(height * 0.96)), outline=(198, 165, 90, 220), width=8)
        draw.ellipse((int(width * 0.74), 30, int(width * 0.96), int(height * 0.34)), fill=(215, 182, 104, 110))


def _build_mask_image(template: MockupTemplate) -> Image.Image:
    width, height = template.output_size
    canvas = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(canvas, "RGBA")
    transform = template.transform

    if isinstance(transform.get("stack"), list):
        for row in transform["stack"]:
            if not isinstance(row, dict):
                continue
            pts = _points(row.get("cover_corners"))
            if len(pts) == 4:
                draw.polygon(pts, fill=(255, 255, 255, 190))
    for key in ("cover_corners", "spine_corners", "left_cover_corners", "right_cover_corners"):
        pts = _points(transform.get(key))
        if len(pts) == 4:
            color = (255, 255, 255, 220) if "spine" not in key else (255, 255, 255, 160)
            draw.polygon(pts, fill=color)
    return canvas


def _parse_size(token: str, fallback: tuple[int, int]) -> tuple[int, int]:
    text = str(token).lower().strip()
    if "x" not in text:
        return fallback
    lhs, rhs = text.split("x", 1)
    width = _safe_int(lhs, fallback[0])
    height = _safe_int(rhs, fallback[1])
    return (max(512, width), max(512, height))


def _generate_background_scene(template: MockupTemplate, *, prompt_cfg: dict[str, str], runtime: config.Config) -> Image.Image:
    width, height = _parse_size(prompt_cfg.get("size", "1600x1200"), template.output_size)
    prompt = prompt_cfg.get("prompt") or (
        f"product photography background scene for {template.id}, no books, center area clear for product placement"
    )
    negative = prompt_cfg.get("negative", "text, watermark, person, hand, book")
    provider = str(runtime.ai_provider or "").strip().lower()
    request_w, request_h = width, height
    if provider == "openai":
        if abs(width - height) <= 50:
            request_w, request_h = 1024, 1024
        elif width > height:
            request_w, request_h = 1536, 1024
        else:
            request_w, request_h = 1024, 1536

    try:
        image_bytes = image_generator.generate_image(
            prompt=prompt,
            negative_prompt=negative,
            model=runtime.ai_model,
            params={
                "provider": provider or runtime.ai_provider,
                "width": request_w,
                "height": request_h,
                "allow_synthetic_fallback": True,
            },
        )
        generated = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
        if generated.size != template.output_size:
            generated = generated.resize(template.output_size, Image.LANCZOS)
        return generated
    except Exception as exc:  # pragma: no cover - defensive fallback
        logger.warning("Background generation failed for %s: %s", template.id, exc)
        fallback = _build_base_scene(template)
        return fallback.resize(template.output_size, Image.LANCZOS)


def generate_backgrounds(*, force: bool = False) -> dict[str, Any]:
    templates = [template for template in load_templates() if template.id in LIFESTYLE_TEMPLATES]
    summary = ensure_template_assets(templates=templates, force=force, generate_backgrounds=True)
    summary["generated_backgrounds"] = len(templates)
    return summary


def _resolve_base_image(template: MockupTemplate) -> Path:
    for suffix in (".png", ".jpg", ".jpeg", ".webp"):
        custom = CUSTOM_BACKGROUNDS_DIR / f"{template.id}_custom{suffix}"
        if custom.exists():
            return custom
    return template.base_image


def _extract_cover_regions(cover: Image.Image, *, spine_width_px: int) -> tuple[Image.Image, Image.Image, Image.Image, Image.Image]:
    width, height = cover.size
    front = cover.crop((width // 2, 0, width, height)).convert("RGB")
    back = cover.crop((0, 0, width // 2, height)).convert("RGB")

    spine_half = max(12, spine_width_px // 2)
    center = width // 2
    spine = cover.crop((max(0, center - spine_half), 0, min(width, center + spine_half), height)).convert("RGB")

    front_w, front_h = front.size
    radius = int(min(front_w, front_h) * 0.22)
    cx = int(front_w * 0.52)
    cy = int(front_h * 0.52)
    detail = front.crop((max(0, cx - radius), max(0, cy - radius), min(front_w, cx + radius), min(front_h, cy + radius))).convert("RGB")

    return front, spine, back, detail


def _points(raw: Any) -> list[tuple[int, int]]:
    if not isinstance(raw, list):
        return []
    out: list[tuple[int, int]] = []
    for row in raw:
        if not isinstance(row, list) or len(row) < 2:
            return []
        out.append((_safe_int(row[0], 0), _safe_int(row[1], 0)))
    return out


def _warp_image(source: Image.Image, corners: list[tuple[int, int]], canvas_size: tuple[int, int]) -> Image.Image:
    if len(corners) != 4:
        return Image.new("RGBA", canvas_size, (0, 0, 0, 0))

    source_rgba = source.convert("RGBA")

    if cv2 is not None:
        src = np.asarray(source_rgba, dtype=np.uint8)
        src_bgra = cv2.cvtColor(src, cv2.COLOR_RGBA2BGRA)
        h, w = src_bgra.shape[:2]

        src_pts = np.float32([[0, 0], [w - 1, 0], [w - 1, h - 1], [0, h - 1]])
        dst_pts = np.float32(corners)
        matrix = cv2.getPerspectiveTransform(src_pts, dst_pts)

        warped_bgra = cv2.warpPerspective(
            src_bgra,
            matrix,
            canvas_size,
            flags=cv2.INTER_CUBIC,
            borderMode=cv2.BORDER_CONSTANT,
            borderValue=(0, 0, 0, 0),
        )
        warped_rgba = cv2.cvtColor(warped_bgra, cv2.COLOR_BGRA2RGBA)
        return Image.fromarray(warped_rgba, mode="RGBA")

    src_w, src_h = source_rgba.size
    src_quad = [(0, 0), (src_w - 1, 0), (src_w - 1, src_h - 1), (0, src_h - 1)]
    coeffs = _find_perspective_coeffs(corners, src_quad)
    transformed = source_rgba.transform(
        canvas_size,
        Image.Transform.PERSPECTIVE,
        coeffs,
        resample=Image.Resampling.BICUBIC,
        fillcolor=(0, 0, 0, 0),
    )
    return transformed


def _find_perspective_coeffs(
    dst_points: list[tuple[int, int]],
    src_points: list[tuple[int, int]],
) -> list[float]:
    matrix: list[list[float]] = []
    vector: list[float] = []
    for (x_dst, y_dst), (x_src, y_src) in zip(dst_points, src_points):
        matrix.append([x_dst, y_dst, 1, 0, 0, 0, -x_src * x_dst, -x_src * y_dst])
        matrix.append([0, 0, 0, x_dst, y_dst, 1, -y_src * x_dst, -y_src * y_dst])
        vector.append(float(x_src))
        vector.append(float(y_src))

    a = np.asarray(matrix, dtype=float)
    b = np.asarray(vector, dtype=float)
    solved = np.linalg.solve(a, b)
    return solved.tolist()


def _book_shadow(layer: Image.Image, *, offset_x: int, offset_y: int, blur: int, opacity: float) -> Image.Image:
    width, height = layer.size
    alpha = layer.split()[-1]
    shadow = Image.new("RGBA", (width, height), (0, 0, 0, max(0, min(255, int(255 * opacity)))))
    shadow.putalpha(alpha)
    shadow = shadow.filter(ImageFilter.GaussianBlur(radius=max(1, blur)))

    canvas = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    canvas.alpha_composite(shadow, (offset_x, offset_y))
    return canvas


def _apply_lighting(image: Image.Image, *, direction: str, intensity: float) -> Image.Image:
    intensity = max(0.0, min(0.5, float(intensity)))
    if intensity <= 0:
        return image

    width, height = image.size
    overlay = Image.new("L", (width, height), color=0)
    pix = overlay.load()

    for y in range(height):
        for x in range(width):
            if direction in {"left", "top-left"}:
                rx = 1.0 - (x / max(1, width - 1))
            elif direction in {"right", "top-right"}:
                rx = x / max(1, width - 1)
            else:
                rx = 1.0 - abs((x - width / 2) / max(1.0, width / 2))

            if direction.startswith("top") or direction == "top-center":
                ry = 1.0 - (y / max(1, height - 1))
            else:
                ry = 1.0 - abs((y - height / 2) / max(1.0, height / 2))

            value = int(max(0.0, min(255.0, (rx * 0.7 + ry * 0.3) * 255.0 * intensity)))
            pix[x, y] = value

    light_layer = Image.new("RGBA", (width, height), (255, 239, 205, 0))
    light_layer.putalpha(overlay)
    return Image.alpha_composite(image, light_layer)


def _add_page_edge_highlight(image: Image.Image, corners: list[tuple[int, int]]) -> None:
    if len(corners) != 4:
        return
    draw = ImageDraw.Draw(image, "RGBA")
    right_top = corners[1]
    right_bottom = corners[2]
    for idx in range(3):
        offset = idx * 2
        draw.line(
            (
                right_top[0] - offset,
                right_top[1],
                right_bottom[0] - offset,
                right_bottom[1],
            ),
            fill=(245, 241, 232, 120 - idx * 24),
            width=1,
        )


def _draw_text_with_wrap(
    draw: ImageDraw.ImageDraw,
    *,
    text: str,
    box: tuple[int, int, int, int],
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    fill: tuple[int, int, int, int],
    line_spacing: int = 6,
) -> None:
    x0, y0, x1, y1 = box
    max_width = max(10, x1 - x0)
    words = [w for w in text.split() if w]
    lines: list[str] = []
    current: list[str] = []

    for word in words:
        candidate = " ".join(current + [word]).strip()
        bbox = draw.textbbox((0, 0), candidate, font=font)
        if bbox[2] - bbox[0] <= max_width or not current:
            current.append(word)
        else:
            lines.append(" ".join(current))
            current = [word]
    if current:
        lines.append(" ".join(current))

    y = y0
    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        height = bbox[3] - bbox[1]
        if y + height > y1:
            break
        draw.text((x0, y), line, font=font, fill=fill)
        y += height + line_spacing


def _render_template_composite(
    *,
    template: MockupTemplate,
    front: Image.Image,
    spine: Image.Image,
    back: Image.Image,
    detail: Image.Image,
    title: str,
    author: str,
) -> Image.Image:
    base_path = _resolve_base_image(template)
    if not base_path.exists():
        _build_base_scene(template).save(base_path, format="PNG")

    base = Image.open(base_path).convert("RGBA").resize(template.output_size, Image.LANCZOS)
    layer = Image.new("RGBA", template.output_size, (0, 0, 0, 0))

    if template.id == "open_spread":
        left_points = _points(template.transform.get("left_cover_corners"))
        right_points = _points(template.transform.get("right_cover_corners"))
        spine_points = _points(template.transform.get("spine_corners"))

        left_source = ImageEnhance.Brightness(back).enhance(1.05)
        right_source = front
        layer.alpha_composite(_warp_image(left_source, left_points, template.output_size))
        layer.alpha_composite(_warp_image(right_source, right_points, template.output_size))
        if spine_points:
            layer.alpha_composite(_warp_image(spine, spine_points, template.output_size))
        _add_page_edge_highlight(layer, right_points)
        _add_page_edge_highlight(layer, left_points)

    elif template.id == "stack_three":
        stack = template.transform.get("stack", [])
        if isinstance(stack, list):
            for row in stack:
                if not isinstance(row, dict):
                    continue
                cover_pts = _points(row.get("cover_corners"))
                spine_pts = _points(row.get("spine_corners"))
                alpha = max(0.2, min(1.0, float(row.get("alpha", 1.0))))
                cover_layer = _warp_image(front, cover_pts, template.output_size)
                if alpha < 1:
                    cover_layer = ImageEnhance.Brightness(cover_layer).enhance(alpha)
                layer.alpha_composite(cover_layer)
                if spine_pts:
                    spine_layer = _warp_image(spine, spine_pts, template.output_size)
                    if alpha < 1:
                        spine_layer = ImageEnhance.Brightness(spine_layer).enhance(alpha)
                    layer.alpha_composite(spine_layer)
                _add_page_edge_highlight(layer, cover_pts)

    elif template.id == "kindle_tablet":
        screen_pts = _points(template.transform.get("cover_corners"))
        layer.alpha_composite(_warp_image(front, screen_pts, template.output_size))

    elif template.id == "social_card":
        cover_pts = _points(template.transform.get("cover_corners"))
        layer.alpha_composite(_warp_image(front, cover_pts, template.output_size))
        draw = ImageDraw.Draw(base, "RGBA")
        try:
            title_font = ImageFont.truetype("Times New Roman.ttf", 52)
            author_font = ImageFont.truetype("Times New Roman.ttf", 34)
        except OSError:
            title_font = ImageFont.load_default()
            author_font = ImageFont.load_default()
        _draw_text_with_wrap(
            draw,
            text=title,
            box=(500, 120, 1130, 380),
            font=title_font,
            fill=(245, 230, 200, 255),
            line_spacing=10,
        )
        _draw_text_with_wrap(
            draw,
            text=author,
            box=(500, 420, 1130, 560),
            font=author_font,
            fill=(196, 163, 82, 255),
            line_spacing=8,
        )
        draw.text((500, 560), "Alexandria Publishing", fill=(205, 181, 124, 235), font=author_font)

    else:
        cover_pts = _points(template.transform.get("cover_corners"))
        spine_pts = _points(template.transform.get("spine_corners"))
        if cover_pts:
            layer.alpha_composite(_warp_image(front, cover_pts, template.output_size))
            _add_page_edge_highlight(layer, cover_pts)
        if spine_pts:
            layer.alpha_composite(_warp_image(spine, spine_pts, template.output_size))

    shadow_cfg = template.transform.get("shadow", {}) if isinstance(template.transform.get("shadow"), dict) else {}
    shadow = _book_shadow(
        layer,
        offset_x=_safe_int(shadow_cfg.get("offset_x"), 16),
        offset_y=_safe_int(shadow_cfg.get("offset_y"), 20),
        blur=_safe_int(shadow_cfg.get("blur"), 24),
        opacity=float(shadow_cfg.get("opacity", 0.3) or 0.3),
    )

    lighting_cfg = template.transform.get("lighting", {}) if isinstance(template.transform.get("lighting"), dict) else {}
    result = Image.alpha_composite(base, shadow)
    result = Image.alpha_composite(result, layer)
    result = _apply_lighting(
        result,
        direction=str(lighting_cfg.get("direction", "top-left") or "top-left"),
        intensity=float(lighting_cfg.get("intensity", 0.12) or 0.12),
    )

    if template.id == "open_spread":
        draw = ImageDraw.Draw(result, "RGBA")
        draw.line((template.output_size[0] // 2, int(template.output_size[1] * 0.2), template.output_size[0] // 2, int(template.output_size[1] * 0.88)), fill=(30, 26, 20, 90), width=2)
    if template.id == "kindle_tablet":
        result = ImageEnhance.Contrast(result).enhance(1.04)
    if template.id == "social_card":
        result = ImageEnhance.Color(result).enhance(1.08)

    # Keep longest side >= 1200 px.
    longest = max(result.size)
    if longest < 1200:
        ratio = 1200 / float(longest)
        new_size = (int(result.width * ratio), int(result.height * ratio))
        result = result.resize(new_size, Image.LANCZOS)

    return result


def _save_dual(image: Image.Image, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rgb = image.convert("RGB")
    png_path = output_path.with_suffix(".png")
    rgb.save(output_path, format="JPEG", quality=95, optimize=True)
    image.save(png_path, format="PNG")
    return output_path


def generate_mockup(
    cover_image_path: str,
    template_id: str,
    output_path: str,
    spine_width_px: int = 100,
    book_title: str = "",
    book_author: str = "",
) -> str:
    templates = template_map()
    if template_id not in templates:
        raise MockupGenerationError(f"Unknown template: {template_id}")

    template = templates[template_id]
    ensure_template_assets(templates=[template], generate_backgrounds=template.id in LIFESTYLE_TEMPLATES)

    cover = Image.open(cover_image_path).convert("RGB")
    front, spine, back, detail = _extract_cover_regions(cover, spine_width_px=spine_width_px)
    rendered = _render_template_composite(
        template=template,
        front=front,
        spine=spine,
        back=back,
        detail=detail,
        title=book_title,
        author=book_author,
    )
    saved = _save_dual(rendered, Path(output_path))
    return str(saved)


def _book_targets(
    *,
    output_root: Path,
    selections_path: Path,
    books: list[int] | None,
    catalog_path: Path | None = None,
) -> list[tuple[BookRecord, Path]]:
    catalog = load_book_records(catalog_path)
    winners = load_winner_map(selections_path)

    target_numbers = books if books else sorted(winners.keys())
    targets: list[tuple[BookRecord, Path]] = []
    for number in target_numbers:
        if number not in catalog:
            continue
        try:
            cover_path = winner_cover_path(book_number=number, output_root=output_root, catalog=catalog, winner_map=winners)
        except MockupGenerationError as exc:
            logger.warning("Skipping book %s: %s", number, exc)
            continue
        targets.append((catalog[number], cover_path))
    return targets


def generate_all_mockups(
    output_dir: str = "Output Covers",
    selections_path: str = "data/winner_selections.json",
    templates: list[str] | None = None,
    books: list[int] | None = None,
    spine_width_px: int = 100,
) -> dict[str, Any]:
    output_root = _resolve_path(output_dir)
    mockups_root = output_root / "Mockups"
    selections = _resolve_path(selections_path)

    all_templates = template_map()
    template_ids = templates if templates else sorted(all_templates.keys())
    selected_templates = [all_templates[t] for t in template_ids if t in all_templates]
    if not selected_templates:
        raise MockupGenerationError("No valid templates selected")

    ensure_template_assets(templates=selected_templates, generate_backgrounds=True)

    targets = _book_targets(output_root=output_root, selections_path=selections, books=books)
    generated = 0
    failed = 0

    for record, cover_path in targets:
        book_dir = mockups_root / record.folder_name
        book_dir.mkdir(parents=True, exist_ok=True)
        for template in selected_templates:
            out = book_dir / f"{template.id}.jpg"
            try:
                generate_mockup(
                    cover_image_path=str(cover_path),
                    template_id=template.id,
                    output_path=str(out),
                    spine_width_px=spine_width_px,
                    book_title=record.title,
                    book_author=record.author,
                )
                generated += 1
            except Exception as exc:  # pragma: no cover - defensive
                failed += 1
                logger.warning("Mockup failed (%s, %s): %s", record.number, template.id, exc)

    return {
        "books": len(targets),
        "templates": [template.id for template in selected_templates],
        "generated": generated,
        "failed": failed,
        "output": str(mockups_root),
    }


def _render_amazon_main(front: Image.Image) -> Image.Image:
    canvas = Image.new("RGB", (2560, 2560), (255, 255, 255))
    target_h = int(canvas.height * 0.85)
    scale = target_h / float(front.height)
    target_w = int(front.width * scale)
    resized = front.resize((target_w, target_h), Image.LANCZOS)
    x = (canvas.width - target_w) // 2
    y = (canvas.height - target_h) // 2
    canvas.paste(resized, (x, y))
    return canvas


def _render_amazon_back(back: Image.Image) -> Image.Image:
    canvas = Image.new("RGB", (2560, 2560), (255, 255, 255))
    target_h = int(canvas.height * 0.82)
    scale = target_h / float(back.height)
    target_w = int(back.width * scale)
    resized = back.resize((target_w, target_h), Image.LANCZOS)
    x = (canvas.width - target_w) // 2
    y = (canvas.height - target_h) // 2
    canvas.paste(resized, (x, y))
    return canvas


def _render_amazon_spine(spine: Image.Image) -> Image.Image:
    canvas = Image.new("RGB", (2560, 2560), (248, 248, 248))
    target_h = int(canvas.height * 0.88)
    scale = target_h / float(spine.height)
    target_w = max(120, int(spine.width * scale * 1.4))
    resized = spine.resize((target_w, target_h), Image.LANCZOS)
    x = (canvas.width - target_w) // 2
    y = (canvas.height - target_h) // 2
    canvas.paste(resized, (x, y))
    return canvas


def _render_amazon_detail(detail: Image.Image) -> Image.Image:
    canvas = Image.new("RGB", (2560, 2560), (242, 239, 233))
    resized = detail.resize((1800, 1800), Image.LANCZOS)
    mask = Image.new("L", resized.size, 0)
    d = ImageDraw.Draw(mask)
    d.ellipse((0, 0, resized.width - 1, resized.height - 1), fill=255)
    x = (canvas.width - resized.width) // 2
    y = (canvas.height - resized.height) // 2
    canvas.paste(resized, (x, y), mask)
    ring = ImageDraw.Draw(canvas)
    ring.ellipse((x - 16, y - 16, x + resized.width + 16, y + resized.height + 16), outline=(196, 163, 82), width=14)
    return canvas


def generate_amazon_set_for_book(
    *,
    book_number: int,
    output_root: Path,
    selections_path: Path,
    spine_width_px: int = 100,
) -> dict[str, Any]:
    catalog = load_book_records()
    winners = load_winner_map(selections_path)
    if book_number not in catalog:
        raise MockupGenerationError(f"Book {book_number} is not in catalog")

    record = catalog[book_number]
    cover_path = winner_cover_path(book_number=book_number, output_root=output_root, catalog=catalog, winner_map=winners)
    cover = Image.open(cover_path).convert("RGB")
    front, spine, back, detail = _extract_cover_regions(cover, spine_width_px=spine_width_px)

    amazon_dir = output_root / "Amazon" / record.folder_name
    amazon_dir.mkdir(parents=True, exist_ok=True)

    main = _render_amazon_main(front)
    main.save(amazon_dir / "01_main.jpg", format="JPEG", quality=95, optimize=True)

    product_path = generate_mockup(
        cover_image_path=str(cover_path),
        template_id="standing_angled",
        output_path=str(amazon_dir / "02_3d_product.jpg"),
        spine_width_px=spine_width_px,
        book_title=record.title,
        book_author=record.author,
    )
    desk_path = generate_mockup(
        cover_image_path=str(cover_path),
        template_id="desk_scene",
        output_path=str(amazon_dir / "03_lifestyle_desk.jpg"),
        spine_width_px=spine_width_px,
        book_title=record.title,
        book_author=record.author,
    )
    shelf_path = generate_mockup(
        cover_image_path=str(cover_path),
        template_id="bookshelf",
        output_path=str(amazon_dir / "04_lifestyle_shelf.jpg"),
        spine_width_px=spine_width_px,
        book_title=record.title,
        book_author=record.author,
    )

    _render_amazon_back(back).save(amazon_dir / "05_back_cover.jpg", format="JPEG", quality=95, optimize=True)
    _render_amazon_spine(spine).save(amazon_dir / "06_spine.jpg", format="JPEG", quality=95, optimize=True)
    _render_amazon_detail(detail).save(amazon_dir / "07_detail.jpg", format="JPEG", quality=95, optimize=True)

    return {
        "book": book_number,
        "folder": record.folder_name,
        "output": str(amazon_dir),
        "files": [
            "01_main.jpg",
            Path(product_path).name,
            Path(desk_path).name,
            Path(shelf_path).name,
            "05_back_cover.jpg",
            "06_spine.jpg",
            "07_detail.jpg",
        ],
    }


def generate_amazon_sets(
    *,
    output_dir: str = "Output Covers",
    selections_path: str = "data/winner_selections.json",
    books: list[int] | None = None,
    spine_width_px: int = 100,
) -> dict[str, Any]:
    output_root = _resolve_path(output_dir)
    selections = _resolve_path(selections_path)

    winners = load_winner_map(selections)
    target_books = books if books else sorted(winners.keys())
    results = []
    failures = 0

    for book in target_books:
        try:
            results.append(
                generate_amazon_set_for_book(
                    book_number=book,
                    output_root=output_root,
                    selections_path=selections,
                    spine_width_px=spine_width_px,
                )
            )
        except Exception as exc:  # pragma: no cover - defensive
            failures += 1
            logger.warning("Amazon set failed for book %s: %s", book, exc)

    return {
        "books": len(results),
        "failed": failures,
        "output": str(output_root / "Amazon"),
        "results": results,
    }


def build_mockup_zip(*, book_number: int, output_dir: Path, destination: Path | None = None) -> Path:
    catalog = load_book_records()
    record = catalog.get(book_number)
    if not record:
        raise MockupGenerationError(f"Book {book_number} not found")

    mockup_dir = output_dir / "Mockups" / record.folder_name
    if not mockup_dir.exists():
        raise MockupGenerationError(f"Mockups not found for book {book_number}")

    zip_path = destination or (config.TMP_DIR / f"mockups_book_{book_number}.zip")
    zip_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in sorted(mockup_dir.glob("*.jpg")):
            zf.write(file_path, arcname=file_path.name)
    return zip_path


def mockup_status(*, output_dir: Path) -> dict[str, Any]:
    catalog = load_book_records()
    templates = load_templates()
    template_ids = {template.id for template in templates}

    books: list[dict[str, Any]] = []
    for number, record in sorted(catalog.items(), key=lambda item: item[0]):
        folder = output_dir / "Mockups" / record.folder_name
        existing = {path.stem for path in folder.glob("*.jpg")} if folder.exists() else set()
        preview = None
        if folder.exists():
            previews = sorted(folder.glob("standing_front.jpg")) or sorted(folder.glob("*.jpg"))
            if previews:
                preview = str(previews[0].relative_to(PROJECT_ROOT))

        books.append(
            {
                "book": number,
                "title": record.title,
                "author": record.author,
                "folder": record.folder_name,
                "generated": len(existing),
                "missing": sorted(template_ids - existing),
                "preview": preview,
            }
        )

    complete = sum(1 for row in books if row["generated"] >= len(template_ids))
    return {
        "templates": sorted(template_ids),
        "total_books": len(books),
        "complete_books": complete,
        "books": books,
    }


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_csv_ints(token: str | None) -> list[int] | None:
    if not token:
        return None
    out = []
    for piece in str(token).split(","):
        number = _safe_int(piece.strip(), 0)
        if number > 0:
            out.append(number)
    return out or None


def _parse_csv_tokens(token: str | None) -> list[str] | None:
    if not token:
        return None
    out = [part.strip() for part in str(token).split(",") if part.strip()]
    return out or None


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate 3D mockups and Amazon image sets")
    parser.add_argument("--catalog", type=str, default=config.DEFAULT_CATALOG_ID, help="Catalog id from config/catalogs.json")
    parser.add_argument("--book", type=int, default=None, help="Generate for one book number")
    parser.add_argument("--books", type=str, default=None, help="Comma-separated book numbers")
    parser.add_argument("--all-books", action="store_true", help="Generate for all winner-selected books")
    parser.add_argument("--template", type=str, default=None, help="Comma-separated template ids")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--selections", type=Path, default=None)
    parser.add_argument("--spine-width", type=int, default=100)
    parser.add_argument("--amazon-set", action="store_true", help="Generate Amazon 7-image set")
    parser.add_argument("--generate-backgrounds", action="store_true", help="Generate lifestyle backgrounds once")
    parser.add_argument("--force", action="store_true", help="Force regeneration of template assets")

    args = parser.parse_args()
    catalog_id = str(getattr(args, "catalog", config.DEFAULT_CATALOG_ID) or config.DEFAULT_CATALOG_ID)
    runtime = config.get_config(catalog_id)
    selections_path = args.selections or config.winner_selections_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)

    if args.generate_backgrounds:
        summary = generate_backgrounds(force=args.force)
        print(json.dumps(summary, indent=2))
        return 0

    books = _parse_csv_ints(args.books)
    if args.book:
        books = [args.book]
    elif args.all_books:
        books = None

    if args.amazon_set:
        summary = generate_amazon_sets(
            output_dir=str(args.output_dir),
            selections_path=str(selections_path),
            books=books,
            spine_width_px=max(40, args.spine_width),
        )
        print(json.dumps(summary, indent=2))
        return 0

    templates = _parse_csv_tokens(args.template)
    summary = generate_all_mockups(
        output_dir=str(args.output_dir),
        selections_path=str(selections_path),
        templates=templates,
        books=books,
        spine_width_px=max(40, args.spine_width),
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
