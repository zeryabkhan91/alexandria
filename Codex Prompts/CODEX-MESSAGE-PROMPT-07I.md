Fix the ornamental frame bleed-through by generating a high-quality diff-based frame mask and using it for compositing.

**The problem:** The current circle punch (radius 465px) cuts into the ornamental frame at 87% of angles because the frame's inner edge varies from 378–480px. This causes the original cover illustration to show through the ornaments.

**The solution has two parts:**

### Part A — Mask generation script
Create `scripts/generate_frame_mask.py` that:
1. Downloads two source covers from Google Drive (they share the same ornamental frame but have different art inside the medallion)
2. Computes per-pixel channel differences — pixels that differ = art, pixels that match = frame
3. Caps the art region to a circle of 485px (just inside the ornament outer edge at ~500px)
4. Applies morphological cleanup (close + gentle dilation) at quarter resolution
5. Saves the result to `config/frame_mask.png` (3784×2777, grayscale: 255=frame/keep, 0=art/replace)

Run this script once after creating it: `python scripts/generate_frame_mask.py`

### Part B — Compositor code changes
Update `src/cover_compositor.py`:
1. Add `FRAME_MASK_PATH` constant pointing to `config/frame_mask.png`
2. Add `_load_frame_mask()` helper that loads and validates the mask
3. Replace the circle-punch block in `composite_single()`'s else branch to use the frame mask (with circle fallback)
4. Adjust `art_diameter` to `FALLBACK_RADIUS * 2 + ART_BLEED_PX * 2 = 1120px` when using the frame mask

See the attached prompt for exact code changes.

After all changes, verify by generating a cover and confirming the ornamental frame is perfectly intact with no art bleeding through.

```
git add -A && git commit && git push
```
