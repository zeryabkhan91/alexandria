# PROMPT-38 Save Raw Integrity Proof Report

## Release

- Functional hardening commits:
  - `388a5ba` (`Harden Save Raw result integrity`)
  - `d705964` (`Harden Save Raw composite integrity`)
- Latest Railway deployment: `1d8c66a3-b6d2-4d56-9490-50ebc106c124` (`SUCCESS`)
- Live app: [https://web-production-900a7.up.railway.app](https://web-production-900a7.up.railway.app)

## What Was Fixed

The bug had two separate failure classes:

1. Save Raw selection ambiguity:
   - the frontend was not sending an immutable exact selector for the chosen card
   - the backend could fall back to ambiguous or mutable artifacts
   - same-book saves could collide in the same Drive package folder

2. Wrong full-cover reuse:
   - historical/composited rows could still drift through mutable temp composite paths
   - this allowed the saved full cover for one result to resolve to another result’s composite

The shipped fix closes both classes:

- Save Raw now requires an exact immutable selector (`variant`, `model`, `raw_art_path`, `saved_composited_path`) and fails closed on mismatch/ambiguity.
- Save Raw package folders are job-scoped and unique per result.
- Durable saved composites are now trusted only when their provenance manifest matches the job row.
- If the durable saved composite is missing or untrusted, the backend rebuilds it from that row’s own durable raw art instead of reusing mutable temp composites.

## Local Verification

Passed locally:

- `python3 -m py_compile scripts/quality_review.py tests/test_quality_review_utils.py tests/test_quality_review_server_smoke.py`
- `pytest tests/test_quality_review_utils.py tests/test_quality_review_server_smoke.py -q -k 'save_raw or hydrate_serialized_result_paths or verified_saved_composite or untrusted_saved_composite or mismatched_card_selector or unique_packages or pre_normalized_selector'`

Those focused tests cover:

- exact result-card selector enforcement
- ambiguity/mismatch refusal
- unique package foldering for same-book saves
- preference for verified durable saved composites over mutable temp outputs
- automatic repair of untrusted saved composites from the row’s own raw art

## Live End-to-End Proof

Live health at proof time:

- `status: ok`
- `healthy: true`
- `uptime_seconds: 1536`
- `books_cataloged: 2397`

Fresh live proof run used two new Emma jobs on the deployed app:

- Job A: `44485241-6b37-4f51-9a46-56384a4c1156`
  - scene intent: Hartfield gardens
- Job B: `828bd0af-b4d3-4e7f-9594-46d0e519a838`
  - scene intent: Donwell Abbey grounds

Both jobs were generated on live production with:

- book `4`
- model `openrouter/google/gemini-2.5-flash-image`
- Drive cover source for Emma
- `preserve_prompt_text=true`

Then live `POST /api/save-raw` was run for both jobs with the exact immutable selector payload.

### Live Save Raw Results

Job A:

- `status: saved`
- `drive_ok: true`
- `drive_folder_id: 1YOjW_2sEuB2eSeVCSZjtoVoBSPIv4Iv0`
- package folder:
  - `save-raw__44485241-6b37-4f51-9a46-56384a4c1156__variant-1__openrouter__google__gemini-2.5-flash-image`

Job B:

- `status: saved`
- `drive_ok: true`
- `drive_folder_id: 1cqx3ZnkPrxciFifDqgLnCzMwgrEhzfLK`
- package folder:
  - `save-raw__828bd0af-b4d3-4e7f-9594-46d0e519a838__variant-1__openrouter__google__gemini-2.5-flash-image`

### Live Integrity Assertions

All of these passed on the deployed app:

- source composite hashes are distinct across the two jobs
- saved package raw JPG hashes are distinct across the two jobs
- saved package composite JPG hashes are distinct across the two jobs
- saved package composite hash for Job A exactly matches Job A’s own live source composite hash
- saved package composite hash for Job B exactly matches Job B’s own live source composite hash
- Drive folder ids are distinct per result
- package folder names are distinct per result

Hash proof JSON:

- [summary.json](/private/tmp/alexandria-prompt38-save-raw-integrity/tmp/prompt38_browser_proof/live_postfix/summary.json)

Fresh job metadata:

- [fresh_jobs.json](/private/tmp/alexandria-prompt38-save-raw-integrity/tmp/prompt38_browser_proof/live_postfix/fresh_jobs.json)

## Visual Proof

I visually inspected the actual saved package outputs for both jobs. They are clearly different scenes and they match the corresponding saved full-cover hashes above.

Saved full cover A:

![Saved full cover A](/private/tmp/alexandria-prompt38-save-raw-integrity/tmp/prompt38_browser_proof/live_postfix/job-a-package-composite.jpg)

Saved full cover B:

![Saved full cover B](/private/tmp/alexandria-prompt38-save-raw-integrity/tmp/prompt38_browser_proof/live_postfix/job-b-package-composite.jpg)

Saved raw art A:

![Saved raw art A](/private/tmp/alexandria-prompt38-save-raw-integrity/tmp/prompt38_browser_proof/live_postfix/job-a-package-raw.jpg)

Saved raw art B:

![Saved raw art B](/private/tmp/alexandria-prompt38-save-raw-integrity/tmp/prompt38_browser_proof/live_postfix/job-b-package-raw.jpg)

## Honest Boundaries

I cannot honestly claim a mathematical proof that this can “never” regress. What I can claim from the code and live proof is narrower and defensible:

- the previously observed overwrite/mix-up class through shared package folders is eliminated
- the previously observed wrong-full-cover reuse through mutable temp composite paths is eliminated
- Save Raw now fails closed on selector mismatch or ambiguity instead of guessing
- durable saved composites are now provenance-checked and rebuilt from the row’s own raw art when needed

One additional observation from live proof: the mutable temp generated image path (`tmp/generated/.../variant_1.png`) is still job-unsafe for cross-job reasoning, because later jobs can overwrite it. The Save Raw path no longer trusts that temp path when durable artifacts are required. That is the important protection.
