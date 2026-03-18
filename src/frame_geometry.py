"""Shared medallion geometry for standard Alexandria covers.

The current codebase has multiple compositor implementations. The most common
failure mode is that each path uses a different effective opening radius, so
the inserted art can under-fill the medallion on one path while fitting
correctly on another.

This module defines the shared standard-cover geometry and the helper
conversions needed by both JPG-space and Im0-space compositors.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

BASE_COVER_WIDTH = 3784
BASE_COVER_HEIGHT = 2777
BASE_CENTER_X = 2864
BASE_CENTER_Y = 1620
BASE_ART_CLIP_RADIUS = 500
DEFAULT_EDGE_FEATHER_PX = 8
BASE_FRAME_HOLE_RADIUS = BASE_ART_CLIP_RADIUS - DEFAULT_EDGE_FEATHER_PX

STANDARD_SIZE_TOLERANCE_PX = 64
STANDARD_ASPECT_TOLERANCE = 0.02


@dataclass(frozen=True, slots=True)
class MedallionGeometry:
    center_x: int
    center_y: int
    frame_hole_radius: int
    art_clip_radius: int
    width: int
    height: int
    scale_x: float
    scale_y: float
    radius_scale: float


def is_standard_medallion_cover(size: tuple[int, int]) -> bool:
    width, height = int(size[0]), int(size[1])
    if width <= 0 or height <= 0:
        return False
    expected_aspect = BASE_COVER_WIDTH / float(BASE_COVER_HEIGHT)
    actual_aspect = width / float(height)
    return (
        abs(width - BASE_COVER_WIDTH) <= STANDARD_SIZE_TOLERANCE_PX
        and abs(height - BASE_COVER_HEIGHT) <= STANDARD_SIZE_TOLERANCE_PX
        and abs(actual_aspect - expected_aspect) <= STANDARD_ASPECT_TOLERANCE
    )


def resolve_standard_medallion_geometry(size: tuple[int, int]) -> MedallionGeometry:
    return resolve_reference_medallion_geometry(
        size,
        center_x=BASE_CENTER_X,
        center_y=BASE_CENTER_Y,
        radius=BASE_ART_CLIP_RADIUS,
    )


def resolve_reference_medallion_geometry(
    size: tuple[int, int],
    *,
    center_x: int,
    center_y: int,
    radius: int,
    feather_px: int = DEFAULT_EDGE_FEATHER_PX,
) -> MedallionGeometry:
    width, height = int(size[0]), int(size[1])
    scale_x = width / float(BASE_COVER_WIDTH)
    scale_y = height / float(BASE_COVER_HEIGHT)
    radius_scale = min(scale_x, scale_y)
    scaled_radius = max(20, int(round(int(radius) * radius_scale)))
    scaled_feather = max(1, int(round(int(feather_px) * radius_scale)))
    return MedallionGeometry(
        center_x=int(round(int(center_x) * scale_x)),
        center_y=int(round(int(center_y) * scale_y)),
        frame_hole_radius=max(20, scaled_radius - scaled_feather),
        art_clip_radius=scaled_radius,
        width=width,
        height=height,
        scale_x=scale_x,
        scale_y=scale_y,
        radius_scale=radius_scale,
    )


def average_jpg_scale(mapping: dict[str, Any]) -> float:
    return max(
        1e-6,
        (
            float(mapping.get("im0_to_jpg_scale_x", 0.0))
            + float(mapping.get("im0_to_jpg_scale_y", 0.0))
        )
        / 2.0,
    )


def jpg_radius_to_im0(mapping: dict[str, Any], radius_jpg: float) -> int:
    scale = average_jpg_scale(mapping)
    return max(20, int(round(float(radius_jpg) / scale)))


def template_geometry_to_im0(mapping: dict[str, Any], size: tuple[int, int]) -> tuple[int, int]:
    geometry = resolve_standard_medallion_geometry(size)
    return (
        jpg_radius_to_im0(mapping, geometry.frame_hole_radius),
        jpg_radius_to_im0(mapping, geometry.art_clip_radius),
    )
