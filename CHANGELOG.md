# Changelog

All notable changes to Alexandria Cover Designer are documented here.

## [2.0.0] - 2026-02-22

### Prompt 17 - Multi-Catalog and Batch Jobs
- Added multi-catalog runtime support and catalog-aware APIs.
- Added persistent async job queue/state with worker service mode.
- Added SSE/event infrastructure for long-running operations.
- Added book metadata support (tags/notes) and compare/job pages.

### Prompt 18 - Analytics and Reporting
- Added cost ledger analytics (totals, by book, by model, timeline).
- Added budget management with warning and hard-stop controls.
- Added quality analytics (trend/distribution/model comparison).
- Added audit log, completion metrics, and report export/list APIs.

### Prompt 19 - Drive Sync, Exports, and Delivery Automation
- Added `src/drive_manager.py` for push/pull/bidirectional sync orchestration.
- Added platform export modules:
  - `src/export_amazon.py`
  - `src/export_ingram.py`
  - `src/export_social.py`
  - `src/export_web.py`
  - `src/export_utils.py`
- Added `src/delivery_pipeline.py` with delivery status/tracking hooks.
- Added export/delivery/archive/storage endpoints and manifest plumbing.

### Prompt 20 - Scale to 2,500
- Added SQLite schema and bootstrap in `src/database.py`.
- Added pooled DB access layer in `src/db.py`.
- Added repository abstraction with JSON and SQLite implementations in `src/repository.py`.
- Added migration script: `scripts/migrate_to_sqlite.py`.
- Added pagination/filter/sort support across all high-volume list endpoints.
- Added scale/performance fixture and tests (`tests/fixtures/scale`, `tests/test_performance.py`).
- Added load-test utility `scripts/load_test.py`.

### Prompt 21 - Final Production Audit and Hardening
- Added `src/security.py` sanitization/path/key-scrubbing utilities.
- Added response security headers and tightened rate-limit tiers.
- Removed unrestricted static/repo file serving fallback in `scripts/quality_review.py`; now only explicit safe asset roots are served.
- Added safe `/static/*` alias mapping to `src/static/*` with path sanitization.
- Standardized API JSON responses with explicit `success` boolean on all responses.
- Hardened float/input validation for non-finite values (`NaN`, `Infinity`) and null-byte/length checks for non-empty text validators.
- Hardened thumbnail handling to allow only configured image roots and fail closed on non-image sources.
- Fixed delivery completion semantics in `src/delivery_pipeline.py` to evaluate `fully_delivered` against required platforms for each run (including subset-platform deliveries).
- Added explicit gdrive `skipped` state when Drive auto-push is disabled, avoiding false incomplete delivery states.
- Hardened Drive selected-file handling in `src/drive_manager.py` by blocking path traversal outside `output_root` for both local mirror and Google API push paths.
- Expanded Drive sync conflict/skip/error branch tests and social export edge-path tests (catalog+platform normalization+error paths).
- Added dedicated `tests/test_export_utils_module.py` and raised `src/export_utils.py` to full focused coverage.
- Expanded web-export edge-path tests and raised `src/export_web.py` to full focused coverage.
- Hardened `src/config.py` JSON loading to fail closed on invalid config payloads instead of raising unhandled decode errors.
- Added dedicated `tests/test_config_module.py` for catalog resolution, template loading, scope parsing, and runtime method behavior.
- Added startup/runtime validation enhancements in health payloads.
- Added environment validation script `scripts/validate_environment.py`.
- Added API contract test coverage in `tests/test_api_contracts.py`.
- Added docs-to-runtime GET route matrix test (`tests/test_api_docs_route_matrix.py`) to prevent undocumented 5xx regressions.
- Expanded repository and delivery regression tests (`tests/test_database_repository_module.py`, `tests/test_delivery_pipeline_module.py`) for edge-path reliability.
- Updated release docs and deployment docs for v2.0.
- Added post-audit hardening iteration for `src/audit_log.py`, `src/book_metadata.py`, `src/safe_json.py`, `src/prompt_generator.py`, and `src/gdrive_sync.py` edge branches.
- Added/expanded module test files:
  - `tests/test_audit_log.py`
  - `tests/test_book_metadata_module.py`
  - `tests/test_safe_json.py`
  - `tests/test_prompt_generator_module.py`
  - `tests/test_gdrive_sync_module.py`
- Added post-audit hardening iteration for `src/cover_analyzer.py` edge + CLI paths and raised module coverage to full.
- Expanded `tests/test_cover_analyzer_module.py` for template fallback, missing-file/no-JPG errors, rectangle overlays, and CLI/main entrypoint flow.
- Added rollback/error-path regression coverage for `src/database.py` schema initialization.
- Expanded `tests/test_database_repository_module.py` with initialize rollback assertion path.
- Added Amazon export edge-path regression coverage (`font fallback`, `KDP resize bounds`, `missing winner`, and per-book export error aggregation).
- Expanded `tests/test_export_modules.py` and raised `src/export_amazon.py` to full coverage.
- Added Ingram export edge-path regression coverage (`missing reportlab`, `missing winner`, and catalog-level error aggregation).
- Expanded `tests/test_export_modules.py` and raised `src/export_ingram.py` to full coverage.
- Added security helper edge-path coverage for empty-path, range checks, required catalog IDs, and empty API key masking.
- Expanded `tests/test_security_module.py` and raised `src/security.py` to full coverage.
- Added catalog-scoped runtime path helpers in `src/config.py` for `cover_regions`, intelligent prompt artifacts, and winner selections.
- Removed fixed 99-entry validation in `scripts/validate_config.py`; catalog size validation is now dynamic per active catalog config.
- Wired iterate/jobs UIs to backend variant limits (removed hardcoded 20 cap) via `/api/iterate-data` and `/api/jobs` limit metadata.
- Added compatibility fallback for runtime stubs without `catalog_id` in generation/similarity/quality paths.
- Added regression coverage:
  - `tests/test_validate_config_script.py`
  - `tests/test_config_module.py` (catalog-scoped path helpers)
  - `tests/test_quality_review_utils.py` (iterate-data limit + catalog-scoped data paths)
- Normalized catalog-scoped winner/archive path defaults across CLI scripts and utilities:
  - `scripts/export_winners.py`
  - `scripts/archive_non_winners.py`
  - `scripts/generate_catalog.py`
  - `scripts/migrate_to_sqlite.py`
  - `scripts/auto_select_winners.py`
  - `scripts/prepare_print_delivery.py`
  - `src/gdrive_sync.py`
  - `src/mockup_generator.py`
  - `src/social_card_generator.py`
- Updated catalog import flow to write regions directly to catalog-specific path (no default-file overwrite side effect) in `scripts/import_catalog.py`.
- Added regression coverage for catalog-scoped CLI defaults and import behavior in `tests/test_script_catalog_path_defaults.py`.
- Added catalog-aware regeneration wiring:
  - `scripts/regenerate_weak.py` now accepts `--catalog` and configures runtime paths from active catalog.
  - `scripts/quality_review.py` now forwards active catalog id when invoking regeneration subprocess.

### Test and Release Snapshot
- `512` tests passing.
- `97.01%` total `src/` coverage (`--cov-fail-under=85` passes).
- Docker build + runtime health checks pass.

## [1.0.0] - 2026-02-21

### Phase 1: Foundation (Prompts 1A-3B)
- Implemented cover-analysis, prompt-generation, image-generation, quality-gate, compositing, and export core modules.
- Established 3784x2777 / 300 DPI format-preservation path for generated variants.
- Added baseline configuration plumbing and pipeline primitives.

### Phase 2: Orchestration and QA (Prompts 4A-5)
- Added end-to-end orchestration flow and run-state tracking.
- Added review tooling foundation with iteration/review web flows.
- Added visual QA support and quality-score handling for comparison workflows.

### Phase 3: Real Generation and Initial Scale (Prompts 6A-7B)
- Fixed blocking generation issues and prepared real-provider execution paths.
- Ran first real-generation workflow and integrated deployment/runtime setup.
- Scaled to 20-title initial scope and wired Drive sync pathways.

### Phase 4: Scale and Advanced Workflow (Prompts 8A-11D)
- Added scaling controls and workflow support for larger catalogs.
- Added winner-selection and archive/export automation scripts.
- Added intelligent prompting, similarity analysis, and mockup generation modules.
- Expanded web app with history, dashboard, similarity, and mockup pages plus supporting APIs.

### Phase 5: UI polish, hardening, performance, testing (Prompts 12-15)
- Unified UI system and page styles.
- Added API validation/response normalization and cache/error tracking.
- Expanded test suite and CI coverage gate.

### Phase 6: Final Release Hardening (Prompt 16)
- Completed 99-title region coverage and startup health checks.
- Added `/catalogs` page and PDF generation UX.
- Hardened shutdown behavior and Docker runtime defaults.
