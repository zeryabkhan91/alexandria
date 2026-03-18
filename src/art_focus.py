"""Focus-aware crop and fit helpers for medallion illustration placement."""

from __future__ import annotations

from typing import Any

import numpy as np
from PIL import Image, ImageOps

ANALYSIS_MAX_SIDE = 512
INTEREST_BORDER_RATIO = 0.04
INTEREST_PERCENTILE = 72.0
INTEREST_EPSILON = 1e-6
CENTERING_MIN = 0.2
CENTERING_MAX = 0.8


def _prepare_analysis_image(image: Image.Image) -> Image.Image:
    rgb = image.convert("RGB")
    width, height = rgb.size
    max_side = max(width, height)
    if max_side <= ANALYSIS_MAX_SIDE:
        return rgb
    scale = ANALYSIS_MAX_SIDE / float(max_side)
    resized = rgb.resize(
        (
            max(1, int(round(width * scale))),
            max(1, int(round(height * scale))),
        ),
        Image.LANCZOS,
    )
    return resized


def compute_focus_centering(image: Image.Image) -> tuple[tuple[float, float], dict[str, Any]]:
    """Return ``ImageOps.fit`` centering biased toward the visual focus."""

    width, height = image.size
    details: dict[str, Any] = {
        "source_width": int(width),
        "source_height": int(height),
        "analysis_width": int(width),
        "analysis_height": int(height),
        "centering_x": 0.5,
        "centering_y": 0.5,
        "focus_x": 0.5,
        "focus_y": 0.5,
        "confidence": 0.0,
    }
    if width <= 1 or height <= 1:
        return (0.5, 0.5), details

    analysis = _prepare_analysis_image(image)
    analysis_width, analysis_height = analysis.size
    details["analysis_width"] = int(analysis_width)
    details["analysis_height"] = int(analysis_height)

    arr = np.asarray(analysis, dtype=np.float32) / 255.0
    if arr.ndim != 3 or arr.shape[2] != 3:
        return (0.5, 0.5), details

    gray = (arr[:, :, 0] * 0.299) + (arr[:, :, 1] * 0.587) + (arr[:, :, 2] * 0.114)
    saturation = arr.max(axis=2) - arr.min(axis=2)

    grad_x = np.zeros_like(gray)
    grad_y = np.zeros_like(gray)
    grad_x[:, 1:] = np.abs(np.diff(gray, axis=1))
    grad_y[1:, :] = np.abs(np.diff(gray, axis=0))
    edge = np.sqrt((grad_x * grad_x) + (grad_y * grad_y))

    interest = (edge * 0.72) + (saturation * 0.28)
    border_x = int(round(float(analysis_width) * INTEREST_BORDER_RATIO))
    border_y = int(round(float(analysis_height) * INTEREST_BORDER_RATIO))
    if border_x > 0:
        interest[:, :border_x] = 0.0
        interest[:, analysis_width - border_x :] = 0.0
    if border_y > 0:
        interest[:border_y, :] = 0.0
        interest[analysis_height - border_y :, :] = 0.0

    max_interest = float(interest.max())
    if max_interest <= INTEREST_EPSILON:
        return (0.5, 0.5), details

    threshold = float(np.percentile(interest, INTEREST_PERCENTILE))
    weights = np.clip(interest - threshold, 0.0, None)
    weight_sum = float(weights.sum())
    if weight_sum <= INTEREST_EPSILON:
        return (0.5, 0.5), details

    yy, xx = np.mgrid[0:analysis_height, 0:analysis_width]
    focus_x = float((xx * weights).sum() / weight_sum) / max(1.0, float(analysis_width - 1))
    focus_y = float((yy * weights).sum() / weight_sum) / max(1.0, float(analysis_height - 1))
    centering_x = float(np.clip(focus_x, CENTERING_MIN, CENTERING_MAX))
    centering_y = float(np.clip(focus_y, CENTERING_MIN, CENTERING_MAX))

    details.update(
        {
            "centering_x": round(centering_x, 6),
            "centering_y": round(centering_y, 6),
            "focus_x": round(focus_x, 6),
            "focus_y": round(focus_y, 6),
            "confidence": round(weight_sum / float(weights.size), 6),
        }
    )
    return (centering_x, centering_y), details


def crop_square(image: Image.Image) -> tuple[Image.Image, dict[str, Any]]:
    """Crop to a square using the computed visual-focus centering."""

    src = image.copy()
    width, height = src.size
    side = min(width, height)
    if side <= 1:
        return src, {
            "source_width": int(width),
            "source_height": int(height),
            "crop_left": 0,
            "crop_top": 0,
            "crop_size": int(side),
            "centering_x": 0.5,
            "centering_y": 0.5,
            "focus_x": 0.5,
            "focus_y": 0.5,
            "confidence": 0.0,
        }

    centering, details = compute_focus_centering(src)
    left = 0
    top = 0
    if width > side:
        left = int(round((width - side) * float(centering[0])))
    if height > side:
        top = int(round((height - side) * float(centering[1])))
    left = int(np.clip(left, 0, max(0, width - side)))
    top = int(np.clip(top, 0, max(0, height - side)))
    cropped = src.crop((left, top, left + side, top + side))
    details.update(
        {
            "crop_left": int(left),
            "crop_top": int(top),
            "crop_size": int(side),
        }
    )
    return cropped, details


def fit_image(
    image: Image.Image,
    size: tuple[int, int],
    *,
    mode: str | None = None,
) -> tuple[Image.Image, dict[str, Any]]:
    """Fit image to target size using focus-aware centering."""

    centering, details = compute_focus_centering(image)
    fitted = ImageOps.fit(
        image,
        (int(size[0]), int(size[1])),
        method=Image.LANCZOS,
        centering=(float(centering[0]), float(centering[1])),
    )
    if mode and fitted.mode != mode:
        fitted = fitted.convert(mode)
    details.update(
        {
            "target_width": int(size[0]),
            "target_height": int(size[1]),
        }
    )
    return fitted, details
