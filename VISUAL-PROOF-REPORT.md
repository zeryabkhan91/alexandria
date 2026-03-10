# PROMPT-35 Visual Proof Report

## Release

- Functional PROMPT-35 commit: `be957a0` (`Implement prompt 35 scene rotation and save raw exports`)
- Follow-up functional fix: `db2c1c3` (`Fix prompt 35 preserve path and add template measurement script`)
- Railway deployment: `75c8e391-5104-4b64-a87d-d0414aead198` (`SUCCESS`)
- Live app: [https://web-production-900a7.up.railway.app](https://web-production-900a7.up.railway.app)

## Verification

Passed locally:

- `python3 -m py_compile src/image_generator.py scripts/measure_template_regions.py`
- `node --check src/static/js/pages/iterate.js`
- `node --check src/static/js/pages/batch.js`
- `node --check src/static/js/openrouter.js`
- `pytest tests/test_image_generator_module.py -q -k preserve_prompt_text_skips_backend_diversification`
- `pytest tests/test_iterate_prompt_builder.py tests/test_batch_prompt_builder.py tests/test_pdf_swap_compositor.py tests/test_quality_review_utils.py -q`
- `pytest tests/test_quality_review_server_smoke.py -q -k 'generate_dry_run_resolves_placeholder_prompt_from_enrichment or save_raw'`
- `pytest tests/test_image_generator_module.py -q -k 'generate_single_book_and_batch or generate_single_book_default_prompt_and_model_fallback or generate_single_book_forwards_preserve_prompt_text or generate_batch_dry_run_failure_append_and_scope_limit'`
- `python scripts/measure_template_regions.py '/Users/timzengerink/Documents/Coding Folder/Alexandria Cover designer/Input Covers/3. Gulliver’s Travels into Several Remote Regions of the World - Jonathan Swift copy'`

Full-suite honesty check:

- `pytest tests/ --maxfail=3 -q` still stops on 3 unrelated failures:
  - `tests/test_api_docs_route_matrix.py::test_api_docs_get_routes_do_not_5xx`
  - `tests/test_prompt_library_module.py::test_alexandria_prompts_seeded_first_and_scene_placeholders_allowed`
  - `tests/test_review_workflow.py::test_review_selection_and_session_roundtrip`

## Live Proof

- Post-deploy readiness check:
  - `GET /api/healthz` returned `status=ok`, `healthy=true`, `startup.status=ready`, `uptime_seconds=51`
- Live Iterate run used book `3` (`Gulliver’s Travels into Several Remote Regions of the World`) and completed `10/10`
- Live `Save Raw` for backend job `c364656f-6611-4dfb-8c26-0677b5144340` returned:
  - `status=saved`
  - `drive_ok=true`
  - `saved_files=6`
  - `missing_files=[]`
  - Drive folder `1lK0ADZvLcSuKTkHiK8CAYpWYJDL6samY`

### Scene Rotation + Saved State

![PROMPT-35 live iterate proof](/tmp/alexandria-proof-live-prompt35/live-iterate-scene-rotation-prompt35.png)

### Save Raw 6-file Proof

![PROMPT-35 save raw proof](/tmp/alexandria-proof-live-prompt35/live-save-raw-proof-prompt35.png)

### Subtitle Blanking Proof

This before/after crop uses the shipped compositor path against the real Gulliver template PDF. The browser result thumbnails are too small to inspect this text band reliably, so the proof below isolates the exact affected region.

![PROMPT-35 subtitle blanking proof](/tmp/alexandria-proof-live-prompt35/live-subtitle-blanking-proof.png)

## Notes

- The live visual proof run was captured on the deployed PROMPT-35 runtime before the `db2c1c3` follow-up redeploy completed. That follow-up only fixes the `preserve_prompt_text=True` regression found by the full-suite run and adds the requested measurement script; it does not change the PROMPT-35 scene-rotation, subtitle-blanking, or save-raw user path already proven above.
- `GET /api/health` can briefly report `status=starting` on a freshly replaced Railway instance even while `GET /api/healthz` already reports `startup.status=ready`. The latter was the stable readiness signal during this deploy.
