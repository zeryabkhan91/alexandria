from __future__ import annotations

from PIL import Image, ImageDraw

from src import art_focus


def test_compute_focus_centering_biases_toward_visual_subject() -> None:
    image = Image.new("RGB", (240, 140), (12, 18, 40))
    draw = ImageDraw.Draw(image)
    draw.rectangle((150, 35, 218, 110), fill=(240, 220, 70))

    centering, details = art_focus.compute_focus_centering(image)

    assert centering[0] > 0.5
    assert float(details["focus_x"]) > 0.5
    assert float(details["confidence"]) > 0.0


def test_crop_square_uses_focus_aware_offset() -> None:
    image = Image.new("RGB", (220, 120), (8, 12, 30))
    draw = ImageDraw.Draw(image)
    draw.rectangle((148, 40, 208, 100), fill=(250, 230, 80))

    cropped, details = art_focus.crop_square(image)

    assert cropped.size == (120, 120)
    assert int(details["crop_left"]) > 50
    assert cropped.getpixel((90, 60))[0] > 200
