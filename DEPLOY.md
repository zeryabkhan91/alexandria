# DEPLOY.md - Alexandria Cover Designer v2.0.0

## 1. Prerequisites
- Docker 24+ and Docker Compose.
- Python 3.11+ (for local non-Docker runs).
- API keys for enabled providers (`OPENROUTER_API_KEY`, `OPENAI_API_KEY`, `GOOGLE_API_KEY`, etc.).
- Optional: Google Drive credentials JSON for Drive sync.

## 2. Environment Setup
1. Copy template:
```bash
cp .env.example .env
```
2. Set required values in `.env`.
3. Recommended production toggles:
```bash
USE_SQLITE=true
SQLITE_DB_PATH=data/alexandria.db
JOB_WORKER_MODE=external
JOB_WORKERS=2
```

## 3. Validation (Required Before Deploy)
Run from repo root:
```bash
.venv/bin/python scripts/validate_config.py
.venv/bin/python scripts/validate_environment.py
.venv/bin/pytest tests --maxfail=1
.venv/bin/pytest --cov=src --cov-config=/dev/null --cov-fail-under=85 -q
python3 -m compileall src scripts
```

## 4. Multi-Catalog Configuration
Catalog definitions live in `config/catalogs.json`.
- Each catalog can point to dedicated catalog/prompt/input/output paths.
- API/UI catalog selector uses this file.
- Keep catalog IDs lowercase and URL-safe.

## 5. SQLite Setup and Migration
1. Enable SQLite:
```bash
export USE_SQLITE=true
```
2. Migrate existing JSON runtime data:
```bash
.venv/bin/python scripts/migrate_to_sqlite.py --catalog classics --db-path data/alexandria.db
```
3. Validate schema/readability:
```bash
sqlite3 data/alexandria.db '.tables'
```

## 6. Google Drive Setup (Optional)
1. Enable Drive API in Google Cloud.
2. Place credentials file at `config/credentials.json` or set `GOOGLE_CREDENTIALS_PATH`.
3. Configure folder IDs (`GDRIVE_OUTPUT_FOLDER_ID` and optional subfolders).
4. Verify from API:
- `GET /api/drive/status`
- `POST /api/drive/push`
- `POST /api/drive/sync`

## 7. Docker Deploy
### Build
```bash
docker build -t alexandria-cover-designer:v2 .
```

### Run single container
```bash
docker run -d -p 8001:8001 --name designer \
  --env-file .env \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/config:/app/config" \
  -v "$(pwd)/Output Covers:/app/Output Covers" \
  alexandria-cover-designer:v2
```

### Run with compose
```bash
docker compose up -d --build
```

## 8. Post-Deploy Checks
- `GET /api/health` returns HTTP 200.
- `GET /api/version` returns `2.0.0`.
- `GET /iterate` loads.
- `GET /api/docs` loads.
- No traceback during stop (`docker stop designer`).

## 9. Monitoring and Alerting
Minimum production signals:
- `/api/health` (liveness/readiness).
- `/api/metrics` (error counters, cache, job telemetry).
- `/api/workers` (worker heartbeat).
- `/api/analytics/budget` (spend guardrails).

Alert suggestions:
- health status not healthy/degraded for >5m.
- worker heartbeat stale.
- sustained 5xx spikes.
- budget threshold >80% or hard-stop reached.

## 10. Backup and Restore
- Backup script paths: `scripts/disaster_recovery.py` + `src/disaster_recovery.py`.
- Backup what matters:
  - `data/` (including SQLite and logs)
  - `config/`
  - `Output Covers/`
- Suggested cadence: nightly snapshot + pre-release snapshot.
- Validate backups by running restore to a staging path and checking `/api/health`.

## 11. Production Checklist
- [ ] `.env` configured with valid provider keys.
- [ ] `validate_config.py` pass.
- [ ] `validate_environment.py` pass.
- [ ] tests and coverage gate pass.
- [ ] SQLite migration complete (if `USE_SQLITE=true`).
- [ ] Drive credentials configured (if Drive features enabled).
- [ ] Docker build/run verified.
- [ ] Backup/restore rehearsal completed.
