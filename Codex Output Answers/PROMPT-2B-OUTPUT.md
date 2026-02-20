# PROMPT-2B OUTPUT — Quality Gate (Scoring + Filtering)

## Summary
Implemented a full automated quality gate in `src/quality_gate.py` with technical scoring, color compatibility checks, artifact detection, diversity scoring, retry-with-tweaked-prompt logic, and multi-model leaderboard aggregation.

## Files Modified
- `src/quality_gate.py`

## Features Implemented
- `QualityScore` dataclass with per-image metrics and pass/fail recommendation.
- `score_image()` technical + color + artifact scoring (0-1 aggregate).
- `score_batch()` over generated folders supporting both layouts:
  - `tmp/generated/{book}/variant_{n}.png`
  - `tmp/generated/{book}/{model}/variant_{n}.png`
- Diversity enforcement using perceptual hash distances; identical sets flagged `not_diverse`.
- Retry strategy (same model, tweaked prompt, up to 3 retries).
- Output writers:
  - `data/quality_scores.json`
  - `data/quality_report.md`
  - `data/retry_log.json`
  - `data/model_rankings.json`
- Human-readable model leaderboard and per-book summary report.
- CLI entrypoint: `python -m src.quality_gate`.

## Verification Checklist
1. `py_compile` passes — **PASS**
2. Score known-good generated image → score >= 0.7 — **PASS** (`0.7536`)
3. Score blank/solid-color image → score < 0.3 — **PASS** (`0.2004`)
4. Score all images for one book (5 variants) and generate report — **PASS**
5. Diversity check flags 5 identical images as not diverse — **PASS**
6. `data/quality_scores.json` valid JSON with entries — **PASS**
7. `data/quality_report.md` readable and accurate — **PASS**

## Notes
- Scoring calibration was tuned to keep sea-heavy classical scenes (for titles like Moby Dick) above threshold while still heavily penalizing blank/solid outputs.
