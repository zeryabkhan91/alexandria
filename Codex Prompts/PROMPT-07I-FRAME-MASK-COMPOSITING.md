# PROMPT-07I — Pixel-Perfect Frame Mask Compositing

## Goal
Replace the broken circle-punch compositing with a high-quality diff-based frame mask that perfectly preserves the ornamental frame.

## Root Cause
The current `composite_single()` draws a circle of `TEMPLATE_PUNCH_RADIUS = 465px` to punch through the cover. But the ornamental frame's inner edge varies from 378–480px, so the circle cuts into the ornaments at 87% of angles. The existing `config/frame_mask.png` also has quality issues (false frame pixels inside the art area).

## Solution (Two Parts)

### Part A: Generate a high-quality frame mask from source covers

Create `scripts/generate_frame_mask.py` that compares two source covers (which share the same ornamental frame but have different art) to produce a pixel-perfect mask.

**Create this new file** `scripts/generate_frame_mask.py`:

```python
#!/usr/bin/env python3
"""
Generate a pixel-perfect frame mask by comparing two source covers.
Pixels that are identical across covers = frame (keep).
Pixels that differ = art area (replace with generated art).

Run once:  python scripts/generate_frame_mask.py
Output:    config/frame_mask.png  (3784x2777, grayscale, 255=frame, 0=art)
"""
import os, sys, gc, time, io, json
import numpy as np
from PIL import Image, ImageFilter

# ── Configuration ──
CX, CY = 2864, 1620          # Medallion center
CAP_RADIUS = 485              # Cap to stay inside ornament outer edge (~500px)
DIFF_THRESHOLD = 10           # Channel-diff above this = art pixel
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "frame_mask.png")

# ── Google Drive helpers ──
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "AIzaSyAY6XvPxrdS_fMNMZEUkJd7UW9b9yuJDgI")
DRIVE_FOLDER_ID = "1ybFYDJk7Y3VlbsEjRAh1LOfdyVsHM_cS"

def list_drive_covers():
    """List JPEG cover files from the shared Google Drive folder."""
    import urllib.request, json as _json
    url = (
        f"https://www.googleapis.com/drive/v3/files"
        f"?q=%27{DRIVE_FOLDER_ID}%27+in+parents+and+mimeType+%3D+%27application%2Fvnd.google-apps.folder%27"
        f"&fields=files(id,name)"
        f"&pageSize=50"
        f"&key={GOOGLE_API_KEY}"
    )
    resp = urllib.request.urlopen(url)
    folders = _json.loads(resp.read())["files"]
    
    covers = []
    for folder in folders[:5]:  # Check first 5 book folders
        furl = (
            f"https://www.googleapis.com/drive/v3/files"
            f"?q=%27{folder['id']}%27+in+parents+and+name+contains+%27.jpg%27"
            f"&fields=files(id,name,size)"
            f"&key={GOOGLE_API_KEY}"
        )
        resp2 = urllib.request.urlopen(furl)
        files = _json.loads(resp2.read())["files"]
        for f in files:
            if f["name"].lower().endswith(".jpg") and not f["name"].startswith("."):
                covers.append(f)
                break  # One cover per folder
        if len(covers) >= 2:
            break
    return covers

def download_drive_file(file_id):
    """Download a file from Google Drive by ID."""
    import urllib.request
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media&key={GOOGLE_API_KEY}"
    resp = urllib.request.urlopen(url)
    return resp.read()


def generate_mask():
    t0 = time.time()
    
    # Step 1: Get two source covers from Drive
    print("Listing source covers from Google Drive...", flush=True)
    covers_meta = list_drive_covers()
    if len(covers_meta) < 2:
        print("ERROR: Need at least 2 source covers on Google Drive.", file=sys.stderr)
        sys.exit(1)
    
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
    
    # Step 2: Compute per-pixel max channel difference
    diff = np.max(np.abs(a1 - a2), axis=2).astype(np.uint8)
    del a1, a2
    gc.collect()
    print(f"Max diff: {diff.max()}", flush=True)
    
    # Step 3: Create cap circle (only process within medallion region)
    yy = np.arange(full_h).reshape(-1, 1).astype(np.float32)
    xx = np.arange(full_w).reshape(1, -1).astype(np.float32)
    in_cap = ((xx - CX)**2 + (yy - CY)**2) <= CAP_RADIUS**2
    del yy, xx
    gc.collect()
    
    # Step 4: Art = diff above threshold AND within cap circle
    art_raw = ((diff > DIFF_THRESHOLD) & in_cap).astype(np.uint8) * 255
    del diff, in_cap
    gc.collect()
    
    # Step 5: Morphological cleanup at 1/4 resolution
    qw, qh = full_w // 4, full_h // 4
    art_q = Image.fromarray(art_raw, mode="L").resize((qw, qh), Image.NEAREST)
    del art_raw
    gc.collect()
    
    # Close (fill small holes): 3x dilate + 3x erode with 3x3 kernel
    t = art_q
    for _ in range(3):
        t = t.filter(ImageFilter.MaxFilter(3))
    for _ in range(3):
        t = t.filter(ImageFilter.MinFilter(3))
    # Safety dilation: 1 pass (~4px at full res)
    t = t.filter(ImageFilter.MaxFilter(3))
    # Smooth edges before upscale
    t = t.filter(ImageFilter.GaussianBlur(radius=1))
    
    # Step 6: Upscale to full resolution
    art_full = t.resize((full_w, full_h), Image.LANCZOS)
    art_arr = (np.array(art_full) > 127).astype(np.uint8) * 255
    
    # Step 7: Frame mask (invert): 255 = frame (keep), 0 = art (replace)
    frame_mask = 255 - art_arr
    
    # Apply subtle Gaussian blur for smooth alpha transition
    fm = Image.fromarray(frame_mask, mode="L").filter(ImageFilter.GaussianBlur(radius=1.5))
    
    # Save
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    fm.save(OUTPUT_PATH)
    print(f"Saved frame mask to {OUTPUT_PATH} ({time.time()-t0:.1f}s)", flush=True)
    
    # Verify
    arr = np.array(fm)
    art_count = np.sum(arr < 128)
    total = full_w * full_h
    print(f"Art area: {art_count}/{total} pixels ({art_count/total*100:.1f}%)")
    print(f"Frame area: {total - art_count}/{total} pixels ({(total-art_count)/total*100:.1f}%)")

if __name__ == "__main__":
    generate_mask()
```

### Part B: Update `cover_compositor.py` to use the frame mask

**File:** `src/cover_compositor.py`

#### Change 1: Add frame mask constant (near line 48, after `ART_BLEED_PX`)

**After this line:**
```python
ART_BLEED_PX = 60  # Extra px on each side beyond punch radius to cover AA transition
```

**Add:**
```python
FRAME_MASK_PATH = Path(__file__).resolve().parent.parent / "config" / "frame_mask.png"
```

#### Change 2: Add frame mask loader (after the `_load_global_compositing_mask` function, around line 1335)

**Add this new function:**
```python
def _load_frame_mask(size: tuple[int, int]) -> Image.Image | None:
    """Load config/frame_mask.png as a grayscale alpha mask.
    
    Returns an 'L' mode image where 255 = frame (opaque) and
    0 = art area (transparent punch). Returns None if unavailable.
    """
    if not FRAME_MASK_PATH.exists():
        logger.warning("frame_mask.png not found at %s", FRAME_MASK_PATH)
        return None
    try:
        mask = Image.open(FRAME_MASK_PATH).convert("L")
    except Exception:
        logger.warning("Failed to load frame mask at %s", FRAME_MASK_PATH)
        return None
    if mask.size != size:
        mask = mask.resize(size, Image.LANCZOS)
    # Sanity check: reject trivially all-white or all-black masks
    arr = np.array(mask, dtype=np.uint8)
    if int(arr.min()) >= 250 or int(arr.max()) <= 5:
        logger.warning("frame_mask.png appears trivially uniform — ignoring")
        return None
    return mask
```

#### Change 3: Replace the circle-punch block in the `else` branch of `composite_single()`

**Replace lines 757-772** (from `# ── 1. Build the template in memory ──` through `template.putalpha(mask)`) **with:**

```python
        # ── 1. Build the template using frame mask or fallback circle ──
        cover_rgba = cover.convert("RGBA")
        tmpl_w, tmpl_h = cover_rgba.size

        frame_mask = _load_frame_mask((tmpl_w, tmpl_h))
        if frame_mask is not None:
            # frame_mask: 255=keep (frame), 0=replace (art hole)
            mask = frame_mask
            logger.info("Using pixel-perfect frame mask from %s", FRAME_MASK_PATH)
        else:
            # Fallback: 4x supersampled anti-aliased circle mask
            logger.warning("frame_mask.png unavailable, falling back to circle punch r=%d", punch_radius)
            scale = 4
            mask_large = Image.new("L", (tmpl_w * scale, tmpl_h * scale), 255)
            draw_mask = ImageDraw.Draw(mask_large)
            cx_s, cy_s, r_s = center_x * scale, center_y * scale, punch_radius * scale
            draw_mask.ellipse((cx_s - r_s, cy_s - r_s, cx_s + r_s, cy_s + r_s), fill=0)
            mask = mask_large.resize((tmpl_w, tmpl_h), Image.LANCZOS)

        template = cover_rgba.copy()
        template.putalpha(mask)
```

#### Change 4: Adjust art_diameter when using frame mask

**Replace line 786** (`art_diameter = punch_radius * 2 + (ART_BLEED_PX * 2)`) **with:**

```python
        # When using frame mask, the art area extends up to ~485px from center.
        # Use FALLBACK_RADIUS (500px) + bleed for full coverage.
        if frame_mask is not None:
            art_diameter = FALLBACK_RADIUS * 2 + (ART_BLEED_PX * 2)  # 500*2+120 = 1120px
        else:
            art_diameter = punch_radius * 2 + (ART_BLEED_PX * 2)  # 465*2+120 = 1050px
```

Note: `frame_mask` is already in scope from the template-building block above, since both blocks are inside the same `else` branch.

## Execution Order

1. Run the mask generation script first:
   ```bash
   python scripts/generate_frame_mask.py
   ```
   This overwrites `config/frame_mask.png` with the high-quality diff-based mask.

2. Apply the code changes to `src/cover_compositor.py`.

3. Test by generating a cover for any book and verifying:
   - The ornamental frame is perfectly intact (no art bleeding through)
   - The generated art fills the medallion completely
   - Output dimensions are 3784×2777

## Why This Works
- Comparing two source covers reveals exactly which pixels are art (they differ between books) vs. frame (identical across all books)
- The cap circle (485px) prevents the diff from including noise outside the frame
- Morphological close fills any small holes where the two covers' art coincidentally matched
- The frame mask follows the exact ornament contours, so the template layer preserves every gold detail
- Falls back to the old circle punch if the mask is ever missing

## Important
- Do NOT change `TEMPLATE_PUNCH_RADIUS` — it's still used as fallback
- Do NOT change any other compositing branches (rectangle, custom_mask)
- Make sure `from pathlib import Path` is already imported (it is, at the top of the file)
- The script needs internet access to download covers from Google Drive

```
git add -A && git commit && git push
```
