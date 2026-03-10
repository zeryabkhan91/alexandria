# PROMPT-37 Visual Proof Report

## Release

- Functional PROMPT-37 commit: `3bfe55d` (`Fix scene rotation through queued generation`)
- Follow-up live fix: `c1ca621` (`Fix scene override binding in generate route`)
- Railway deployment: `e900c55b-4bb2-4bc6-bde7-bc29046d6bfe` (`SUCCESS`)
- Live app: [https://web-production-900a7.up.railway.app](https://web-production-900a7.up.railway.app)

## Verification

Passed locally:

- `python3 -m py_compile scripts/quality_review.py src/image_generator.py`
- `pytest tests/test_quality_review_utils.py -q -k 'ensure_prompt_book_context_rotates_scene_anchor_for_variant or execute_generation_payload_honors_scene_description_for_variant_prompt or ensure_enriched_prompt_replaces_generic_scene_and_mood'`
- `pytest tests/test_image_generator_module.py -q -k 'validate_prompt_relevance_prepends_title_when_missing or validate_prompt_relevance_uses_variant_scene_anchor_from_enrichment'`
- `pytest tests/test_iterate_prompt_builder.py -q`
- `pytest tests/test_batch_prompt_builder.py -q`
- `pytest tests/test_quality_review_utils.py -q -k 'save_raw_payload_for_job_returns_partial_when_drive_upload_fails or save_raw_helpers_do_not_fallback_to_directory_scans'`
- `pytest tests/test_quality_review_utils.py -q -k 'execute_generation_payload_honors_scene_description_for_variant_prompt or execute_generation_payload_sanitizes_unresolved_placeholders_before_generation or execute_generation_payload_appends_enrichment_for_generic_prompt'`
- `pytest tests/test_quality_review_server_smoke.py -q -k 'generate_dry_run_resolves_placeholder_prompt_from_enrichment or generate_dry_run_preserves_precomposed_prompt_text or generate_dry_run_appends_enrichment_for_generic_prompt'`

Catalog coverage check:

- `python scripts/validate_prompt_resolution.py`
  - result: all `2397` books resolved with usable enrichment

Full-suite honesty:

- `pytest tests/ --maxfail=3 -q` was started as an honesty check but did not finish in a reasonable window. I am not claiming a clean full-suite pass for PROMPT-37.

## Live Proof

- Post-fix health:
  - `GET /api/health` returned `status=ok`, `healthy=true`, `books_cataloged=2397`
- Live Iterate proof used book `3` (`Gulliver’s Travels into Several Remote Regions of the World`) with `BASE 4 — Romantic Realism`
- The fixed deployed Iterate flow showed distinct queued scene snippets for the same prompt family:
  - beach + Lilliputians
  - Emperor of Lilliput palace
  - Glumdalclitch carrying Gulliver
  - King of Brobdingnag
  - Houyhnhnms pasture
  - Yahoos scene
- A real live OpenRouter Flash job completed after the fix:
  - job id: `838c1465-80ec-4c5c-85d0-3a4898cf1d5c`
  - model: `openrouter/google/gemini-2.5-flash-image`
  - `status=completed`
  - `compositor_mode=pdf`
  - `generation_time=12.96s`
- Real live `POST /api/save-raw` for that completed job returned:
  - `status=saved`
  - `drive_ok=true`
  - `saved_files=6`
  - `drive_uploaded=6`
  - `drive_folder_id=1lK0ADZvLcSuKTkHiK8CAYpWYJDL6samY`

### Live Iterate Config

![PROMPT-37 live iterate config proof](/tmp/alexandria-proof-live-prompt37/live-iterate-config-prompt37.png)

### Live Scene Variation

![PROMPT-37 live scene variation proof](/tmp/alexandria-proof-live-prompt37/live-iterate-scene-variation-prompt37.png)

### Live Save Raw Proof

![PROMPT-37 live save raw proof](/tmp/alexandria-proof-live-prompt37/live-save-raw-proof-prompt37.png)

### Live Composited Cover

![PROMPT-37 live composited cover proof](/tmp/alexandria-proof-live-prompt37/live-cover-book3-prompt37.jpg)

## Notes

- Honest live issue observed during proof: the direct Google route `google/gemini-2.5-flash-image` currently fails in production with `403 PERMISSION_DENIED` because the deployed Google API key was reported leaked. That is a provider credential issue, not a PROMPT-37 scene/save-raw regression.
- The OpenRouter Gemini Flash route worked end to end on the same deployment, so PROMPT-37 itself is verified live on the deployed app.
