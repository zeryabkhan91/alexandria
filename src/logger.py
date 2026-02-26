"""Central logging configuration for Alexandria Cover Designer."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "pipeline.log"

_STANDARD_RECORD_FIELDS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
}

_COLOR_MAP = {
    "DEBUG": "\033[36m",
    "INFO": "\033[32m",
    "WARNING": "\033[33m",
    "ERROR": "\033[31m",
    "CRITICAL": "\033[35m",
}
_COLOR_RESET = "\033[0m"


class JsonLogFormatter(logging.Formatter):
    """Write JSON logs for file output."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "module": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _STANDARD_RECORD_FIELDS or key.startswith("_"):
                continue
            payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class ColoredConsoleFormatter(logging.Formatter):
    """Human-readable colorized console logs."""

    def format(self, record: logging.LogRecord) -> str:
        color = _COLOR_MAP.get(record.levelname, "")
        level = f"{color}{record.levelname}{_COLOR_RESET}" if color else record.levelname
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
        message = record.getMessage()
        return f"[{timestamp}] [{level}] {record.name}: {message}"


_CONFIGURED = False


def configure_logging() -> None:
    """Configure root logger once with rotating JSON file + colored console."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(JsonLogFormatter())

    console_level = os.getenv("LOG_LEVEL_CONSOLE", "INFO").upper()
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, console_level, logging.INFO))
    console_handler.setFormatter(ColoredConsoleFormatter())

    root.addHandler(file_handler)
    root.addHandler(console_handler)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Get module logger with project logging configured."""
    configure_logging()
    return logging.getLogger(name)
