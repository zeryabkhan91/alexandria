#!/usr/bin/env python3
"""Validate runtime environment prerequisites for Alexandria Cover Designer."""

from __future__ import annotations

import importlib.metadata
import os
import platform
import shutil
import socket
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import config  # noqa: E402


def _read_requirements(path: Path) -> list[str]:
    if not path.exists():
        return []
    rows: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        token = line.split(";", 1)[0].strip()
        for marker in ["==", ">=", "<=", "~=", ">", "<"]:
            if marker in token:
                token = token.split(marker, 1)[0].strip()
                break
        if token:
            rows.append(token)
    return sorted(set(rows))


def _print_row(label: str, ok: bool, detail: str) -> None:
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {label}: {detail}")


def _warn_row(label: str, detail: str) -> None:
    print(f"[WARN] {label}: {detail}")


def _check_dns(host: str) -> bool:
    try:
        socket.getaddrinfo(host, 443)
        return True
    except OSError:
        return False


def main() -> int:
    failures: list[str] = []
    runtime = config.get_config()

    py_ok = sys.version_info >= (3, 11)
    _print_row("Python >= 3.11", py_ok, platform.python_version())
    if not py_ok:
        failures.append("Python 3.11+ is required")

    requirements = _read_requirements(PROJECT_ROOT / "requirements.txt")
    missing_packages: list[str] = []
    for package in requirements:
        if package == "pathlib2" and sys.version_info >= (3, 6):
            # Backport package not required on modern Python.
            continue
        try:
            importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            missing_packages.append(package)
    _print_row(
        "Dependencies installed",
        len(missing_packages) == 0,
        "all present" if not missing_packages else ", ".join(missing_packages[:20]),
    )
    if missing_packages:
        failures.append("Missing pip dependencies")

    dirs_to_check = {
        "config": config.CONFIG_DIR,
        "data": config.DATA_DIR,
        "output": config.OUTPUT_DIR,
        "tmp": config.TMP_DIR,
    }
    for name, path in dirs_to_check.items():
        exists = path.exists()
        writable = exists and os.access(path, os.W_OK)
        _print_row(f"Directory {name}", exists and writable, str(path))
        if not (exists and writable):
            failures.append(f"Directory not writable: {path}")

    free_gb = shutil.disk_usage(PROJECT_ROOT).free / (1024 ** 3)
    disk_ok = free_gb >= 10.0
    _print_row("Disk space >= 10GB", disk_ok, f"{free_gb:.2f} GB free")
    if not disk_ok:
        failures.append("Insufficient disk space (<10GB)")

    provider_hosts = {
        "openrouter": "openrouter.ai",
        "openai": "api.openai.com",
        "google": "generativelanguage.googleapis.com",
        "fal": "fal.run",
        "replicate": "api.replicate.com",
    }
    for provider, host in provider_hosts.items():
        has_key = bool(runtime.provider_keys.get(provider, "").strip())
        if not has_key:
            _warn_row(f"Provider connectivity ({provider})", "API key not configured; skipping reachability check")
            continue
        reachable = _check_dns(host)
        _print_row(f"Provider connectivity ({provider})", reachable, host)
        if not reachable:
            failures.append(f"Cannot resolve provider host: {host}")

    # Optional Drive credentials warning only.
    drive_credentials = Path(config.GOOGLE_CREDENTIALS_PATH) if config.GOOGLE_CREDENTIALS_PATH else (config.CONFIG_DIR / "credentials.json")
    if drive_credentials.exists():
        _print_row("Google Drive credentials", True, str(drive_credentials))
    else:
        _warn_row("Google Drive credentials", f"not found at {drive_credentials}; Drive sync endpoints will fail until configured")

    if failures:
        print("\nEnvironment validation failed:")
        for issue in failures:
            print(f"- {issue}")
        return 1

    print("\nEnvironment validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
