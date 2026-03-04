#!/usr/bin/env python3
"""
Generate a pixel-perfect frame mask by comparing two source covers.
Pixels that are identical across covers = frame (keep).
Pixels that differ = art area (replace with generated art).

Run once:  python scripts/generate_frame_mask.py
Output:    config/frame_mask.png  (3784x2777, grayscale, 255=frame, 0=art)
"""

import gc
import io
import os
import sys
import time

import numpy as np
from PIL import Image, ImageFilter

# Configuration
CX, CY = 2864, 1620
CAP_RADIUS = 485
DIFF_THRESHOLD = 10
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "frame_mask.png")

# Google Drive helpers
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "AIzaSyAY6XvPxrdS_fMNMZEUkJd7UW9b9yuJDgI")
DRIVE_FOLDER_ID = "1ybFYDJk7Y3VlbsEjRAh1LOfdyVsHM_cS"


def list_drive_covers() -> list[dict[str, str]]:
    """List JPEG cover files from the shared Google Drive folder."""
    import json as _json
    import urllib.request

    url = (
        "https://www.googleapis.com/drive/v3/files"
        f"?q=%27{DRIVE_FOLDER_ID}%27+in+parents+and+mimeType+%3D+%27application%2Fvnd.google-apps.folder%27"
        "&fields=files(id,name)"
        "&pageSize=50"
        f"&key={GOOGLE_API_KEY}"
    )
    response = urllib.request.urlopen(url, timeout=60)
    folders = _json.loads(response.read())["files"]

    covers: list[dict[str, str]] = []
    for folder in folders[:5]:
        folder_url = (
            "https://www.googleapis.com/drive/v3/files"
            f"?q=%27{folder['id']}%27+in+parents+and+name+contains+%27.jpg%27"
            "&fields=files(id,name,size)"
            f"&key={GOOGLE_API_KEY}"
        )
        response2 = urllib.request.urlopen(folder_url, timeout=60)
        files = _json.loads(response2.read())["files"]
        for item in files:
            name = str(item.get("name", ""))
            if name.lower().endswith(".jpg") and not name.startswith("."):
                covers.append(item)
                break
        if len(covers) >= 2:
            break
    return covers


def download_drive_file(file_id: str) -> bytes:
    """Download a file from Google Drive by file ID."""
    import urllib.request

    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media&key={GOOGLE_API_KEY}"
    response = urllib.request.urlopen(url, timeout=120)
    return response.read()


def generate_mask() -> None:
    start = time.time()

    print("Listing source covers from Google Drive...", flush=True)
    covers_meta = list_drive_covers()
    if len(covers_meta) < 2:
        print("ERROR: Need at least 2 source covers on Google Drive.", file=sys.stderr)
        raise SystemExit(1)

    print(f"Downloading {covers_meta[0]['name']}...", flush=True)
    data1 = download_drive_file(covers_meta[0]["id"])
    a1 = np.array(Image.open(io.BytesIO(data1)).convert("RGB"), dtype=np.int16)
    del data1

    print(f"Downloading {covers_meta[1]['name']}...", flush=True)
    data2 = download_drive_file(covers_meta[1]["id"])
    a2 = np.array(Image.open(io.BytesIO(data2)).convert("RGB"), dtype=np.int16)
    del data2
    gc.collect()

    full_h, full_w = a1.shape[:2]
    print(f"Cover size: {full_w}x{full_h}", flush=True)

    diff = np.max(np.abs(a1 - a2), axis=2).astype(np.uint8)
    del a1, a2
    gc.collect()
    print(f"Max diff: {int(diff.max())}", flush=True)

    yy = np.arange(full_h).reshape(-1, 1).astype(np.float32)
    xx = np.arange(full_w).reshape(1, -1).astype(np.float32)
    in_cap = ((xx - CX) ** 2 + (yy - CY) ** 2) <= CAP_RADIUS**2
    del yy, xx
    gc.collect()

    art_raw = ((diff > DIFF_THRESHOLD) & in_cap).astype(np.uint8) * 255
    del diff, in_cap
    gc.collect()

    quarter_w, quarter_h = full_w // 4, full_h // 4
    art_q = Image.fromarray(art_raw, mode="L").resize((quarter_w, quarter_h), Image.NEAREST)
    del art_raw
    gc.collect()

    temp = art_q
    for _ in range(3):
        temp = temp.filter(ImageFilter.MaxFilter(3))
    for _ in range(3):
        temp = temp.filter(ImageFilter.MinFilter(3))
    temp = temp.filter(ImageFilter.MaxFilter(3))
    temp = temp.filter(ImageFilter.GaussianBlur(radius=1))

    art_full = temp.resize((full_w, full_h), Image.LANCZOS)
    art_arr = (np.array(art_full) > 127).astype(np.uint8) * 255
    frame_mask = 255 - art_arr
    frame_mask_img = Image.fromarray(frame_mask, mode="L").filter(ImageFilter.GaussianBlur(radius=1.5))

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    frame_mask_img.save(OUTPUT_PATH)
    print(f"Saved frame mask to {OUTPUT_PATH} ({time.time() - start:.1f}s)", flush=True)

    arr = np.array(frame_mask_img, dtype=np.uint8)
    art_count = int(np.sum(arr < 128))
    total = full_w * full_h
    frame_count = total - art_count
    print(f"Art area: {art_count}/{total} pixels ({(art_count / total) * 100:.1f}%)")
    print(f"Frame area: {frame_count}/{total} pixels ({(frame_count / total) * 100:.1f}%)")


if __name__ == "__main__":
    generate_mask()
