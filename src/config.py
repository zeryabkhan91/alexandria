"""Central runtime configuration for Alexandria Cover Designer."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

try:
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    from logger import get_logger  # type: ignore

load_dotenv()
logger = get_logger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = PROJECT_ROOT / os.getenv("INPUT_DIR", "Input Covers")
OUTPUT_DIR = PROJECT_ROOT / os.getenv("OUTPUT_DIR", "Output Covers")
TMP_DIR = PROJECT_ROOT / os.getenv("TMP_DIR", "tmp")
DATA_DIR = PROJECT_ROOT / os.getenv("DATA_DIR", "data")
CONFIG_DIR = PROJECT_ROOT / os.getenv("CONFIG_DIR", "config")

PROMPTS_PATH = CONFIG_DIR / os.getenv("PROMPTS_FILE", "book_prompts.json")
BOOK_CATALOG_PATH = CONFIG_DIR / os.getenv("BOOK_CATALOG_FILE", "book_catalog.json")
PROMPT_TEMPLATES_PATH = CONFIG_DIR / os.getenv("PROMPT_TEMPLATES_FILE", "prompt_templates.json")
PROMPT_LIBRARY_PATH = CONFIG_DIR / os.getenv("PROMPT_LIBRARY_FILE", "prompt_library.json")
CATALOGS_PATH = CONFIG_DIR / os.getenv("CATALOGS_FILE", "catalogs.json")
COVER_TEMPLATES_PATH = CONFIG_DIR / os.getenv("COVER_TEMPLATES_FILE", "cover_templates.json")
MODEL_PROMPT_OVERRIDES_PATH = CONFIG_DIR / os.getenv("MODEL_PROMPT_OVERRIDES_FILE", "model_prompt_overrides.json")
DEFAULT_CATALOG_ID = os.getenv("CATALOG_ID", "classics").strip() or "classics"

# Provider defaults
AI_PROVIDER = os.getenv("AI_PROVIDER", "openrouter").strip().lower()
AI_MODEL = os.getenv("AI_MODEL", "openrouter/google/gemini-2.5-flash-image").strip()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
FAL_API_KEY = os.getenv("FAL_API_KEY", "")
# Deprecated/inactive provider (kept for compatibility only).
REPLICATE_API_TOKEN = os.getenv("REPLICATE_API_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "anthropic").strip().lower()
LLM_MODEL = os.getenv("LLM_MODEL", "claude-sonnet-4-5-20250929").strip()
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "2000"))
LLM_COST_PER_1K_TOKENS = float(os.getenv("LLM_COST_PER_1K_TOKENS", "0.003"))
GDRIVE_OUTPUT_FOLDER_ID = os.getenv("GDRIVE_OUTPUT_FOLDER_ID", "1CWxCE3dP2AmQRy-w9MowP0J7AWNuAYdW")
GDRIVE_SOURCE_FOLDER_ID = os.getenv("GDRIVE_SOURCE_FOLDER_ID", "").strip()
GDRIVE_INPUT_FOLDER_ID = os.getenv("GDRIVE_INPUT_FOLDER_ID", GDRIVE_SOURCE_FOLDER_ID).strip()
GDRIVE_MOCKUPS_FOLDER_ID = os.getenv("GDRIVE_MOCKUPS_FOLDER_ID", "")
GDRIVE_AMAZON_FOLDER_ID = os.getenv("GDRIVE_AMAZON_FOLDER_ID", "")
GDRIVE_SOCIAL_FOLDER_ID = os.getenv("GDRIVE_SOCIAL_FOLDER_ID", "")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
WEBHOOK_EVENTS = [token.strip() for token in os.getenv("WEBHOOK_EVENTS", "batch_complete,batch_error,milestone").split(",") if token.strip()]
OUTBOUND_ALLOWLIST_DOMAINS = [
    token.strip().lower()
    for token in os.getenv(
        "OUTBOUND_ALLOWLIST_DOMAINS",
        "api.openai.com,openrouter.ai,fal.run,generativelanguage.googleapis.com",
    ).split(",")
    if token.strip()
]

ALL_MODELS = [
    m.strip()
    for m in os.getenv(
        "ALL_MODELS",
        "openrouter/google/gemini-2.5-flash-image,openrouter/openai/gpt-5-image-mini,openrouter/openai/gpt-5-image,fal/fal-ai/flux-2/klein/4b,fal/fal-ai/flux-2,openai/gpt-image-1-mini,openai/gpt-image-1,google/gemini-2.5-flash-image,google/gemini-3-pro-image-preview",
    ).split(",")
    if m.strip()
]

MODEL_PROVIDER_MAP: dict[str, str] = {
    "flux-2-pro": "openrouter",
    "flux-2-schnell": "openrouter",
    "gpt-image-1-high": "openai",
    "gpt-image-1-medium": "openai",
    "imagen-4-ultra": "google",
    "imagen-4-fast": "google",
    "nano-banana-pro": "openrouter",
    "openrouter/google/gemini-2.5-flash-image": "openrouter",
    "openrouter/google/gemini-3-pro-image-preview": "openrouter",
    "openrouter/openai/gpt-5-image-mini": "openrouter",
    "openrouter/openai/gpt-5-image": "openrouter",
    "fal/fal-ai/flux-2/klein/4b": "fal",
    "fal/fal-ai/flux-2": "fal",
    "fal/fal-ai/flux-2-pro": "fal",
    "openai/gpt-image-1-mini": "openai",
    "openai/gpt-image-1": "openai",
    "openai/gpt-image-1.5": "openai",
    "google/imagen-4.0-fast-generate-001": "google",
    "google/imagen-4.0-generate-001": "google",
    "google/imagen-4.0-ultra-generate-001": "google",
}

MODEL_COST_USD: dict[str, float] = {
    "flux-2-pro": 0.055,
    "flux-2-schnell": 0.003,
    "gpt-image-1-high": 0.167,
    "gpt-image-1-medium": 0.040,
    "imagen-4-ultra": 0.060,
    "imagen-4-fast": 0.030,
    "nano-banana-pro": 0.067,
    "google/gemini-2.5-flash-image": 0.003,
    "google/gemini-3-pro-image-preview": 0.02,
    "openai/gpt-5-image-mini": 0.012,
    "openai/gpt-5-image": 0.04,
    "fal-ai/flux-2/klein/4b": 0.0025,
    "fal-ai/flux-2": 0.012,
    "fal-ai/flux-2-pro": 0.045,
    "gpt-image-1-mini": 0.01,
    "gpt-image-1": 0.04,
    "gpt-image-1.5": 0.06,
    "imagen-4.0-fast-generate-001": 0.03,
    "imagen-4.0-generate-001": 0.06,
    "imagen-4.0-ultra-generate-001": 0.08,
}

VARIANTS_PER_COVER = int(os.getenv("VARIANTS_PER_COVER", "5"))
MAX_GENERATION_VARIANTS = int(os.getenv("MAX_GENERATION_VARIANTS", "50"))
BATCH_CONCURRENCY = int(os.getenv("BATCH_CONCURRENCY", "2"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.0"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
PROVIDER_CIRCUIT_FAILURE_THRESHOLD = int(os.getenv("PROVIDER_CIRCUIT_FAILURE_THRESHOLD", "3"))
PROVIDER_CIRCUIT_COOLDOWN_SECONDS = float(os.getenv("PROVIDER_CIRCUIT_COOLDOWN_SECONDS", "90"))

PROVIDER_REQUEST_DELAY = {
    "openrouter": float(os.getenv("OPENROUTER_REQUEST_DELAY", str(REQUEST_DELAY))),
    "fal": float(os.getenv("FAL_REQUEST_DELAY", str(REQUEST_DELAY))),
    "replicate": float(os.getenv("REPLICATE_REQUEST_DELAY", str(REQUEST_DELAY))),
    "openai": float(os.getenv("OPENAI_REQUEST_DELAY", str(REQUEST_DELAY))),
    "google": float(os.getenv("GOOGLE_REQUEST_DELAY", str(REQUEST_DELAY))),
}

RATE_LIMITS_PER_SECOND = {
    "openrouter": int(os.getenv("RATE_LIMIT_OPENROUTER", "10")),
    "fal": int(os.getenv("RATE_LIMIT_FAL", "5")),
    "replicate": int(os.getenv("RATE_LIMIT_REPLICATE", "10")),
    "google": int(os.getenv("RATE_LIMIT_GOOGLE", "5")),
    # OpenAI image generation is mainly minute-bounded in this project.
    "openai": int(os.getenv("RATE_LIMIT_OPENAI_PER_SECOND", "0")),
}

RATE_LIMITS_PER_MINUTE = {
    "openrouter": int(os.getenv("RATE_LIMIT_OPENROUTER_PER_MIN", "200")),
    "openai": int(os.getenv("RATE_LIMIT_OPENAI", "5")),
    "fal": int(os.getenv("RATE_LIMIT_FAL_PER_MIN", "0")),
    "replicate": int(os.getenv("RATE_LIMIT_REPLICATE_PER_MIN", "0")),
    "google": int(os.getenv("RATE_LIMIT_GOOGLE_PER_MIN", "0")),
}

GEN_WIDTH = int(os.getenv("GEN_WIDTH", "1024"))
GEN_HEIGHT = int(os.getenv("GEN_HEIGHT", "1024"))
GEN_OUTPUT_FORMAT = os.getenv("GEN_OUTPUT_FORMAT", "png").strip().lower()

MIN_QUALITY_SCORE = float(os.getenv("MIN_QUALITY_SCORE", "0.6"))
MAX_COST_USD = float(os.getenv("MAX_COST_USD", "200.00"))

BOOK_SCOPE_LIMIT = int(os.getenv("BOOK_SCOPE_LIMIT", "20"))
MAX_EXPORT_VARIANTS = int(os.getenv("MAX_EXPORT_VARIANTS", "20"))
COMPOSITE_MAX_INVALID_VARIANTS = int(os.getenv("COMPOSITE_MAX_INVALID_VARIANTS", "0"))
BORDER_STRIP_PERCENT = float(os.getenv("BORDER_STRIP_PERCENT", "0.05"))
SLO_WINDOW_DAYS = int(os.getenv("SLO_WINDOW_DAYS", "7"))
SLO_ALERT_COOLDOWN_SECONDS = int(os.getenv("SLO_ALERT_COOLDOWN_SECONDS", "900"))
SLO_ALERT_LEVELS = [
    token.strip().lower() for token in os.getenv("SLO_ALERT_LEVELS", "breached,at_risk").split(",") if token.strip()
]
SLO_MONITOR_INTERVAL_SECONDS = int(os.getenv("SLO_MONITOR_INTERVAL_SECONDS", "300"))
JOB_WORKERS = int(os.getenv("JOB_WORKERS", "2"))
JOB_WORKER_MODE = os.getenv("JOB_WORKER_MODE", "external").strip().lower() or "external"
JOB_WORKER_HEARTBEAT_PATH = PROJECT_ROOT / os.getenv("JOB_WORKER_HEARTBEAT_PATH", "data/worker_heartbeat.json")
JOB_WORKER_HEARTBEAT_STALE_SECONDS = int(os.getenv("JOB_WORKER_HEARTBEAT_STALE_SECONDS", "120"))
ALLOW_SYNC_GENERATION = os.getenv("ALLOW_SYNC_GENERATION", "0").strip().lower() in {"1", "true", "yes", "on"}
JOB_STALE_RECOVERY_SECONDS = int(os.getenv("JOB_STALE_RECOVERY_SECONDS", "900"))
JOB_STALE_RECOVERY_RETRY_DELAY_SECONDS = float(os.getenv("JOB_STALE_RECOVERY_RETRY_DELAY_SECONDS", "2.0"))

FAILURES_PATH = DATA_DIR / "generation_failures.json"
GENERATION_PLAN_PATH = DATA_DIR / "generation_plan.json"
GENERATION_STATE_PATH = DATA_DIR / "generation_state.json"
STATE_DB_PATH = DATA_DIR / os.getenv("STATE_DB_PATH", "state.sqlite3")
USE_SQLITE = os.getenv("USE_SQLITE", "0").strip().lower() in {"1", "true", "yes", "on"}
SQLITE_DB_PATH = DATA_DIR / os.getenv("SQLITE_DB_PATH", "alexandria.db")


@dataclass(slots=True)
class CatalogConfig:
    id: str
    name: str
    book_count: int
    catalog_file: Path
    prompts_file: Path
    input_covers_dir: Path
    output_covers_dir: Path
    cover_style: str = "navy_gold_medallion"
    status: str = "active"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "book_count": self.book_count,
            "catalog_file": str(self.catalog_file),
            "prompts_file": str(self.prompts_file),
            "input_covers_dir": str(self.input_covers_dir),
            "output_covers_dir": str(self.output_covers_dir),
            "cover_style": self.cover_style,
            "status": self.status,
        }


def ensure_runtime_dirs() -> None:
    """Ensure runtime directories exist."""
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    logger.debug(
        "Ensured runtime directories",
        extra={"tmp_dir": str(TMP_DIR), "data_dir": str(DATA_DIR), "config_dir": str(CONFIG_DIR)},
    )


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _resolve_project_path(token: str | Path) -> Path:
    path = Path(token)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _sanitize_all_models(models: list[str]) -> list[str]:
    seen: set[str] = set()
    cleaned: list[str] = []
    for raw in models:
        token = str(raw or "").strip()
        if not token:
            continue
        if token.lower().startswith("replicate/"):
            continue
        if token in seen:
            continue
        seen.add(token)
        cleaned.append(token)
    return cleaned


def _default_catalog_payload() -> dict[str, Any]:
    return {
        "catalogs": [
            {
                "id": "classics",
                "name": "Alexandria Classics",
                "book_count": 99,
                "catalog_file": "config/book_catalog.json",
                "prompts_file": "config/book_prompts.json",
                "input_covers_dir": "Input Covers",
                "output_covers_dir": "Output Covers",
                "cover_style": "navy_gold_medallion",
                "status": "complete",
            }
        ]
    }


def _load_catalogs_payload() -> dict[str, Any]:
    payload = _load_json(CATALOGS_PATH)
    if isinstance(payload, dict):
        rows = payload.get("catalogs")
        if isinstance(rows, list):
            return payload
        if isinstance(rows, dict):
            normalized_rows: list[dict[str, Any]] = []
            for key, value in rows.items():
                if not isinstance(value, dict):
                    continue
                entry = dict(value)
                entry.setdefault("id", str(key))
                normalized_rows.append(entry)
            return {
                "catalogs": normalized_rows,
                "default_catalog": str(payload.get("default_catalog", DEFAULT_CATALOG_ID)),
            }
    return _default_catalog_payload()


def list_catalogs() -> list[CatalogConfig]:
    payload = _load_catalogs_payload()
    rows = payload.get("catalogs", []) if isinstance(payload, dict) else []
    catalogs: list[CatalogConfig] = []

    for row in rows:
        if not isinstance(row, dict):
            continue

        catalog_id = str(row.get("id", "")).strip()
        if not catalog_id:
            continue

        catalog_file = _resolve_project_path(str(row.get("catalog_file", "config/book_catalog.json")))
        prompts_file = _resolve_project_path(str(row.get("prompts_file", "config/book_prompts.json")))
        input_covers_dir = _resolve_project_path(str(row.get("input_covers_dir", "Input Covers")))
        output_covers_dir = _resolve_project_path(str(row.get("output_covers_dir", "Output Covers")))

        catalogs.append(
            CatalogConfig(
                id=catalog_id,
                name=str(row.get("name", catalog_id)),
                book_count=int(row.get("book_count", 0) or 0),
                catalog_file=catalog_file,
                prompts_file=prompts_file,
                input_covers_dir=input_covers_dir,
                output_covers_dir=output_covers_dir,
                cover_style=str(row.get("cover_style", "navy_gold_medallion") or "navy_gold_medallion"),
                status=str(row.get("status", "active") or "active"),
            )
        )

    if catalogs:
        return catalogs

    fallback = _default_catalog_payload()["catalogs"][0]
    return [
        CatalogConfig(
            id=str(fallback["id"]),
            name=str(fallback["name"]),
            book_count=int(fallback["book_count"]),
            catalog_file=_resolve_project_path(str(fallback["catalog_file"])),
            prompts_file=_resolve_project_path(str(fallback["prompts_file"])),
            input_covers_dir=_resolve_project_path(str(fallback["input_covers_dir"])),
            output_covers_dir=_resolve_project_path(str(fallback["output_covers_dir"])),
            cover_style=str(fallback["cover_style"]),
            status=str(fallback["status"]),
        )
    ]


def get_catalog(catalog_id: str) -> CatalogConfig:
    wanted = str(catalog_id or "").strip().lower()
    if not wanted:
        raise KeyError("catalog_id is empty")

    for catalog in list_catalogs():
        if catalog.id.lower() == wanted:
            return catalog

    available = ", ".join(sorted(catalog.id for catalog in list_catalogs()))
    raise KeyError(f"Catalog '{catalog_id}' not found. Available: {available}")


def resolve_catalog(catalog_id: str | None = None) -> CatalogConfig:
    wanted = (catalog_id or DEFAULT_CATALOG_ID).strip() if catalog_id is not None else DEFAULT_CATALOG_ID
    catalogs = list_catalogs()
    if wanted:
        for catalog in catalogs:
            if catalog.id.lower() == wanted.lower():
                return catalog
    return catalogs[0]


def load_cover_templates(path: Path | None = None) -> dict[str, Any]:
    templates_path = path or COVER_TEMPLATES_PATH
    payload = _load_json(templates_path)
    if isinstance(payload, dict) and isinstance(payload.get("templates"), list):
        return payload
    return {
        "templates": [
            {
                "id": "navy_gold_medallion",
                "description": "Navy background, gold ornaments, circular medallion center-right",
                "region_type": "circle",
                "compositing": "raster_first",
            }
        ]
    }


def _catalog_token(catalog_id: str | None) -> str:
    token = str(catalog_id or DEFAULT_CATALOG_ID).strip().lower()
    if token:
        token = re.sub(r"[^a-z0-9_-]+", "-", token).strip("-_")
        token = token[:120]
    return token or DEFAULT_CATALOG_ID


def catalog_scoped_config_path(filename: str, *, catalog_id: str | None = None, config_dir: Path | None = None) -> Path:
    """Return a catalog-scoped config path (classics keeps unsuffixed legacy name)."""
    root = config_dir or CONFIG_DIR
    name = Path(filename).name
    token = _catalog_token(catalog_id)
    if token == "classics":
        return root / name
    stem = Path(name).stem
    suffix = Path(name).suffix
    return root / f"{stem}_{token}{suffix}"


def catalog_scoped_data_path(filename: str, *, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    """Return a catalog-scoped data path (classics keeps unsuffixed legacy name)."""
    root = data_dir or DATA_DIR
    name = Path(filename).name
    token = _catalog_token(catalog_id)
    if token == "classics":
        return root / name
    stem = Path(name).stem
    suffix = Path(name).suffix
    return root / f"{stem}_{token}{suffix}"


def cover_regions_path(*, catalog_id: str | None = None, config_dir: Path | None = None) -> Path:
    return catalog_scoped_config_path("cover_regions.json", catalog_id=catalog_id, config_dir=config_dir)


def enriched_catalog_path(*, catalog_id: str | None = None, config_dir: Path | None = None) -> Path:
    return catalog_scoped_config_path("book_catalog_enriched.json", catalog_id=catalog_id, config_dir=config_dir)


def intelligent_prompts_path(*, catalog_id: str | None = None, config_dir: Path | None = None) -> Path:
    return catalog_scoped_config_path("book_prompts_intelligent.json", catalog_id=catalog_id, config_dir=config_dir)


def winner_selections_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("winner_selections.json", catalog_id=catalog_id, data_dir=data_dir)


def archive_log_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("archive_log.json", catalog_id=catalog_id, data_dir=data_dir)


def quality_scores_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("quality_scores.json", catalog_id=catalog_id, data_dir=data_dir)


def generation_history_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("generation_history.json", catalog_id=catalog_id, data_dir=data_dir)


def regeneration_results_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("regeneration_results.json", catalog_id=catalog_id, data_dir=data_dir)


def prompt_performance_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("prompt_performance.json", catalog_id=catalog_id, data_dir=data_dir)


def llm_usage_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("llm_usage.json", catalog_id=catalog_id, data_dir=data_dir)


def audit_log_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("audit_log.json", catalog_id=catalog_id, data_dir=data_dir)


def error_metrics_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("error_metrics.json", catalog_id=catalog_id, data_dir=data_dir)


def cost_ledger_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("cost_ledger.json", catalog_id=catalog_id, data_dir=data_dir)


def budget_config_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("budget_config.json", catalog_id=catalog_id, data_dir=data_dir)


def delivery_config_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("delivery_pipeline.json", catalog_id=catalog_id, data_dir=data_dir)


def delivery_tracking_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("delivery_tracking.json", catalog_id=catalog_id, data_dir=data_dir)


def report_schedules_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("report_schedules.json", catalog_id=catalog_id, data_dir=data_dir)


def slo_metrics_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("slo_metrics.json", catalog_id=catalog_id, data_dir=data_dir)


def slo_alert_state_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("slo_alert_state.json", catalog_id=catalog_id, data_dir=data_dir)


def review_data_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("review_data.json", catalog_id=catalog_id, data_dir=data_dir)


def iterate_data_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("iterate_data.json", catalog_id=catalog_id, data_dir=data_dir)


def compare_data_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("compare_data.json", catalog_id=catalog_id, data_dir=data_dir)


def variant_selections_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("variant_selections.json", catalog_id=catalog_id, data_dir=data_dir)


def review_stats_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("review_stats.json", catalog_id=catalog_id, data_dir=data_dir)


def similarity_hashes_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("cover_hashes.json", catalog_id=catalog_id, data_dir=data_dir)


def similarity_matrix_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("similarity_matrix.json", catalog_id=catalog_id, data_dir=data_dir)


def similarity_clusters_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("similarity_clusters.json", catalog_id=catalog_id, data_dir=data_dir)


def similarity_dismissed_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("similarity_dismissed.json", catalog_id=catalog_id, data_dir=data_dir)


def drive_sync_log_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("drive_sync_log.json", catalog_id=catalog_id, data_dir=data_dir)


def drive_schedule_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("drive_schedule.json", catalog_id=catalog_id, data_dir=data_dir)


def batch_runs_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("batch_runs.json", catalog_id=catalog_id, data_dir=data_dir)


def exports_manifest_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("exports_manifest.json", catalog_id=catalog_id, data_dir=data_dir)


def pipeline_state_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("pipeline_state.json", catalog_id=catalog_id, data_dir=data_dir)


def pipeline_summary_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("pipeline_summary.json", catalog_id=catalog_id, data_dir=data_dir)


def pipeline_summary_markdown_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("pipeline_summary.md", catalog_id=catalog_id, data_dir=data_dir)


def gdrive_sync_state_path(*, catalog_id: str | None = None, data_dir: Path | None = None) -> Path:
    return catalog_scoped_data_path("gdrive_sync_state.json", catalog_id=catalog_id, data_dir=data_dir)


def get_initial_scope_book_numbers(limit: int | None = None, *, catalog_id: str | None = None) -> list[int]:
    """Return the first N book numbers from catalog for D23 scope."""
    catalog_cfg = resolve_catalog(catalog_id)
    payload = _load_json(catalog_cfg.catalog_file)
    if not isinstance(payload, list):
        return []

    max_items = limit if limit is not None else BOOK_SCOPE_LIMIT
    values: list[int] = []
    for entry in payload:
        number = entry.get("number")
        if isinstance(number, int):
            values.append(number)
        elif isinstance(number, str) and number.isdigit():
            values.append(int(number))
        if len(values) >= max_items:
            break
    return values


@dataclass(slots=True)
class Config:
    """Typed runtime configuration snapshot."""

    project_root: Path = PROJECT_ROOT
    input_dir: Path = INPUT_DIR
    output_dir: Path = OUTPUT_DIR
    tmp_dir: Path = TMP_DIR
    data_dir: Path = DATA_DIR
    config_dir: Path = CONFIG_DIR

    prompts_path: Path = PROMPTS_PATH
    book_catalog_path: Path = BOOK_CATALOG_PATH
    prompt_templates_path: Path = PROMPT_TEMPLATES_PATH
    prompt_library_path: Path = PROMPT_LIBRARY_PATH
    catalogs_path: Path = CATALOGS_PATH
    cover_templates_path: Path = COVER_TEMPLATES_PATH
    model_prompt_overrides_path: Path = MODEL_PROMPT_OVERRIDES_PATH

    catalog_id: str = DEFAULT_CATALOG_ID
    catalog_name: str = "Alexandria Classics"
    cover_style: str = "navy_gold_medallion"

    ai_provider: str = AI_PROVIDER
    ai_model: str = AI_MODEL
    all_models: list[str] = field(default_factory=lambda: ALL_MODELS.copy())

    openrouter_api_key: str = OPENROUTER_API_KEY
    fal_api_key: str = FAL_API_KEY
    replicate_api_token: str = REPLICATE_API_TOKEN
    openai_api_key: str = OPENAI_API_KEY
    google_api_key: str = GOOGLE_API_KEY
    google_credentials_json: str = GOOGLE_CREDENTIALS_JSON
    anthropic_api_key: str = ANTHROPIC_API_KEY
    llm_provider: str = LLM_PROVIDER
    llm_model: str = LLM_MODEL
    llm_max_tokens: int = LLM_MAX_TOKENS
    llm_cost_per_1k_tokens: float = LLM_COST_PER_1K_TOKENS
    gdrive_output_folder_id: str = GDRIVE_OUTPUT_FOLDER_ID
    gdrive_source_folder_id: str = GDRIVE_SOURCE_FOLDER_ID
    gdrive_input_folder_id: str = GDRIVE_INPUT_FOLDER_ID
    gdrive_mockups_folder_id: str = GDRIVE_MOCKUPS_FOLDER_ID
    gdrive_amazon_folder_id: str = GDRIVE_AMAZON_FOLDER_ID
    gdrive_social_folder_id: str = GDRIVE_SOCIAL_FOLDER_ID
    google_credentials_path: str = GOOGLE_CREDENTIALS_PATH

    webhook_url: str = WEBHOOK_URL
    webhook_events: list[str] = field(default_factory=lambda: WEBHOOK_EVENTS.copy())
    outbound_allowlist_domains: list[str] = field(default_factory=lambda: OUTBOUND_ALLOWLIST_DOMAINS.copy())

    request_delay: float = REQUEST_DELAY
    max_retries: int = MAX_RETRIES
    provider_circuit_failure_threshold: int = PROVIDER_CIRCUIT_FAILURE_THRESHOLD
    provider_circuit_cooldown_seconds: float = PROVIDER_CIRCUIT_COOLDOWN_SECONDS
    batch_concurrency: int = BATCH_CONCURRENCY
    variants_per_cover: int = VARIANTS_PER_COVER
    max_generation_variants: int = MAX_GENERATION_VARIANTS
    provider_request_delay: dict[str, float] = field(default_factory=lambda: PROVIDER_REQUEST_DELAY.copy())
    provider_rate_limit_per_second: dict[str, int] = field(default_factory=lambda: RATE_LIMITS_PER_SECOND.copy())
    provider_rate_limit_per_minute: dict[str, int] = field(default_factory=lambda: RATE_LIMITS_PER_MINUTE.copy())

    image_width: int = GEN_WIDTH
    image_height: int = GEN_HEIGHT
    image_output_format: str = GEN_OUTPUT_FORMAT

    min_quality_score: float = MIN_QUALITY_SCORE
    max_cost_usd: float = MAX_COST_USD

    model_provider_map: dict[str, str] = field(default_factory=lambda: MODEL_PROVIDER_MAP.copy())
    model_cost_usd: dict[str, float] = field(default_factory=lambda: MODEL_COST_USD.copy())

    failures_path: Path = FAILURES_PATH
    generation_plan_path: Path = GENERATION_PLAN_PATH
    generation_state_path: Path = GENERATION_STATE_PATH
    state_db_path: Path = STATE_DB_PATH
    use_sqlite: bool = USE_SQLITE
    sqlite_db_path: Path = SQLITE_DB_PATH

    book_scope_limit: int = BOOK_SCOPE_LIMIT
    max_export_variants: int = MAX_EXPORT_VARIANTS
    composite_max_invalid_variants: int = COMPOSITE_MAX_INVALID_VARIANTS
    border_strip_percent: float = BORDER_STRIP_PERCENT
    slo_window_days: int = SLO_WINDOW_DAYS
    slo_alert_cooldown_seconds: int = SLO_ALERT_COOLDOWN_SECONDS
    slo_alert_levels: list[str] = field(default_factory=lambda: SLO_ALERT_LEVELS.copy())
    slo_monitor_interval_seconds: int = SLO_MONITOR_INTERVAL_SECONDS
    job_workers: int = JOB_WORKERS
    job_worker_mode: str = JOB_WORKER_MODE
    job_worker_heartbeat_path: Path = JOB_WORKER_HEARTBEAT_PATH
    job_worker_heartbeat_stale_seconds: int = JOB_WORKER_HEARTBEAT_STALE_SECONDS
    allow_sync_generation: bool = ALLOW_SYNC_GENERATION
    job_stale_recovery_seconds: int = JOB_STALE_RECOVERY_SECONDS
    job_stale_recovery_retry_delay_seconds: float = JOB_STALE_RECOVERY_RETRY_DELAY_SECONDS

    # Compatibility aliases
    input_covers_dir: Path = INPUT_DIR
    output_covers_dir: Path = OUTPUT_DIR
    cost_per_image_usd: float = MODEL_COST_USD.get(AI_MODEL, 0.04)

    @property
    def provider_keys(self) -> dict[str, str]:
        return {
            "openrouter": self.openrouter_api_key,
            "fal": self.fal_api_key,
            "openai": self.openai_api_key,
            "google": self.google_api_key,
        }

    def has_any_api_key(self) -> bool:
        return any(bool(v.strip()) for v in self.provider_keys.values())

    def get_api_key(self, provider: str) -> str:
        return self.provider_keys.get(provider.lower(), "")

    def resolve_model_provider(self, model: str, default_provider: str | None = None) -> str:
        """Resolve provider for a model, supporting provider/model notation."""
        if "/" in model:
            prefix = model.split("/", 1)[0].strip().lower()
            if prefix in self.provider_keys:
                return prefix
        return self.model_provider_map.get(model, (default_provider or self.ai_provider).lower())

    def get_model_cost(self, model: str) -> float:
        normalized = model.split("/", 1)[-1] if "/" in model else model
        return float(self.model_cost_usd.get(normalized, self.model_cost_usd.get(model, 0.04)))


RuntimeConfig = Config


def get_config(catalog_id: str | None = None) -> Config:
    ensure_runtime_dirs()
    cfg = Config()
    cfg.all_models = _sanitize_all_models(cfg.all_models)
    if str(cfg.ai_model or "").strip().lower().startswith("replicate/"):
        cfg.ai_model = cfg.all_models[0] if cfg.all_models else ""
    cfg.variants_per_cover = max(1, int(cfg.variants_per_cover or 1))
    cfg.max_generation_variants = max(cfg.variants_per_cover, int(cfg.max_generation_variants or cfg.variants_per_cover))
    cfg.max_export_variants = max(1, int(cfg.max_export_variants or 1))
    cfg.composite_max_invalid_variants = max(0, int(cfg.composite_max_invalid_variants or 0))
    cfg.border_strip_percent = max(0.0, min(0.20, float(cfg.border_strip_percent or 0.0)))

    try:
        catalog = resolve_catalog(catalog_id)
        cfg.catalog_id = catalog.id
        cfg.catalog_name = catalog.name
        cfg.cover_style = catalog.cover_style
        cfg.book_catalog_path = catalog.catalog_file
        cfg.prompts_path = catalog.prompts_file
        cfg.input_dir = catalog.input_covers_dir
        cfg.output_dir = catalog.output_covers_dir
        cfg.input_covers_dir = catalog.input_covers_dir
        cfg.output_covers_dir = catalog.output_covers_dir
    except Exception as exc:
        logger.warning("Falling back to default catalog paths: %s", exc)

    logger.debug(
        "Loaded runtime configuration snapshot",
        extra={
            "provider": cfg.ai_provider,
            "model": cfg.ai_model,
            "models": cfg.all_models,
            "catalog_id": cfg.catalog_id,
            "catalog": cfg.catalog_name,
        },
    )
    return cfg
