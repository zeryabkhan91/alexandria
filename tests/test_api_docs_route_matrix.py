from __future__ import annotations

import ast
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.request import urlopen


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_health(base_url: str, timeout_seconds: float = 45.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urlopen(f"{base_url}/api/health", timeout=10) as response:
                if int(getattr(response, "status", 200)) == 200:
                    return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError("quality_review server did not become ready")


def _start_server() -> tuple[subprocess.Popen[bytes], str]:
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    process = subprocess.Popen(
        [sys.executable, "scripts/quality_review.py", "--serve", "--port", str(port)],
        cwd=PROJECT_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=os.environ.copy(),
    )
    try:
        _wait_for_health(base_url)
    except Exception:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        raise
    return process, base_url


def _stop_server(process: subprocess.Popen[bytes]) -> None:
    process.terminate()
    try:
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=8)


def _api_docs_endpoints() -> list[tuple[str, str, str, str, str, str]]:
    source = (PROJECT_ROOT / "scripts" / "quality_review.py").read_text(encoding="utf-8")
    match = re.search(r"def _build_api_docs_html\(\) -> str:\n\s*endpoints = \[(.*?)\n\s*\]\n", source, re.S)
    if not match:
        raise AssertionError("Could not parse _build_api_docs_html endpoint table")
    data = ast.literal_eval(f"[{match.group(1)}]")
    return [tuple(item) for item in data]


def _http_status(base_url: str, path: str) -> int:
    try:
        with urlopen(f"{base_url}{path}", timeout=20) as response:
            return int(getattr(response, "status", 200))
    except HTTPError as exc:
        return int(exc.code)


def test_api_docs_get_routes_do_not_5xx():
    process, base_url = _start_server()
    try:
        endpoints = _api_docs_endpoints()
        tested = 0
        for method, route, _name, _params, _example, _desc in endpoints:
            if method != "GET":
                continue
            if "{" in route or "}" in route:
                continue
            # Known dynamic output artifact path; resolved by other endpoint calls.
            if route.startswith("/api/generate-catalog?"):
                continue
            status = _http_status(base_url, route)
            assert status < 500, f"{route} -> {status}"
            tested += 1
        # Guardrail to ensure this test remains meaningful.
        assert tested >= 35
    finally:
        _stop_server(process)
