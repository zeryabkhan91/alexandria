"""Prompt 2A image generation pipeline with provider abstraction and all-model mode."""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import logging
import random
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import numpy as np
import requests
from PIL import Image, ImageDraw

try:
    from src import config
    from src import safe_json
    from src import similarity_detector
    from src import prompt_generator
    from src.logger import get_logger
    from src.prompt_library import PromptLibrary
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import safe_json  # type: ignore
    import similarity_detector  # type: ignore
    import prompt_generator  # type: ignore
    from logger import get_logger  # type: ignore
    from prompt_library import PromptLibrary  # type: ignore

logger = get_logger(__name__)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

MODEL_STYLE_PROFILES: list[dict[str, str]] = [
    {
        "style": "dramatic cinematic classic",
        "detail": "deep chiaroscuro, controlled golden highlights, tactile brush texture",
        "palette": "deep navy, gold, and warm amber contrast",
        "composition": "full-bleed scene with title-safe center focus",
    },
    {
        "style": "minimal modern literary",
        "detail": "restrained geometry, clean negative space, strong silhouette hierarchy",
        "palette": "off-white base with one bold accent hue",
        "composition": "split layout with asymmetric visual weight",
    },
    {
        "style": "vintage engraved classic",
        "detail": "fine line etching texture, aged paper grain, ornate vignette rhythm",
        "palette": "sepia, umber, and antique brass",
        "composition": "central medallion motif with breathing room around edges",
    },
    {
        "style": "bold graphic poster",
        "detail": "high-contrast blocks, simplified shapes, strong focal tension",
        "palette": "vibrant jewel tones with one dark anchor color",
        "composition": "dominant foreground subject and simplified layered background",
    },
    {
        "style": "ethereal painterly",
        "detail": "soft atmospheric diffusion, subtle edge blending, dreamlike depth",
        "palette": "pastel gradients with desaturated supporting tones",
        "composition": "floating central subject with directional light drift",
    },
    {
        "style": "dark moody gothic",
        "detail": "inky shadows, selective highlights, dramatic depth cues",
        "palette": "deep charcoal with selective neon or gold accents",
        "composition": "close-up focal subject with layered foreground framing",
    },
    {
        "style": "illustrated hand-crafted",
        "detail": "ink-and-wash feel, visible hand-drawn imperfections, textured brush marks",
        "palette": "earth pigments with muted cool balancing tones",
        "composition": "scene-led narrative tableau with diagonal motion",
    },
    {
        "style": "typography-led conceptual cover",
        "detail": "image mass arranged to support bold title area and clear hierarchy",
        "palette": "monochrome base plus one accent color family",
        "composition": "structured geometry with deliberate text-safe negative space",
    },
]

MODEL_PROVIDER_HINTS: tuple[tuple[str, str], ...] = (
    ("midjourney", "Emphasize artistic direction, stylized brushwork, and cinematic composition cues."),
    ("dalle", "Use precise scene layout instructions and clear object placement relationships."),
    ("gpt-image", "Use precise scene layout instructions and clear object placement relationships."),
    ("openai", "Use precise scene layout instructions and clear object placement relationships."),
    ("flux", "Prioritize tactile lighting realism and material texture fidelity."),
    ("gemini", "Lean into conceptual symbolism and non-literal narrative framing."),
    ("stable-diffusion", "Include technical style keywords and painterly medium guidance."),
    ("sdxl", "Include technical style keywords and painterly medium guidance."),
)


def _host_matches_allowlist(host: str, pattern: str) -> bool:
    host_token = str(host or "").strip().lower()
    allow = str(pattern or "").strip().lower()
    if not host_token or not allow:
        return False
    if allow in {"*", "any"}:
        return True
    if allow.startswith("*.") and len(allow) > 2:
        root = allow[2:]
        return host_token == root or host_token.endswith(f".{root}")
    return host_token == allow or host_token.endswith(f".{allow}")


@dataclass(slots=True)
class GenerationResult:
    """Result for one generated image."""

    book_number: int
    variant: int
    prompt: str
    model: str
    image_path: Path | None
    success: bool
    error: str | None
    generation_time: float
    cost: float
    provider: str
    skipped: bool = False
    dry_run: bool = False
    attempts: int = 0
    similarity_warning: str | None = None
    similar_to_book: int | None = None
    distinctiveness_score: float | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["image_path"] = str(self.image_path) if self.image_path else None
        return payload


class GenerationError(Exception):
    """Terminal generation error."""


class RetryableGenerationError(GenerationError):
    """Generation error that should be retried."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class BaseProvider:
    """Provider interface."""

    name = "base"

    def __init__(
        self,
        model: str,
        api_key: str = "",
        timeout: float = 120.0,
        runtime: config.Config | None = None,
    ):
        self.model = model
        self.api_key = api_key
        self.timeout = timeout
        self.runtime = runtime

    def generate(self, prompt: str, negative_prompt: str, width: int, height: int, seed: int | None = None) -> Image.Image:
        raise NotImplementedError

    def _assert_outbound_url(self, url: str) -> None:
        runtime = self.runtime or config.get_config()
        allowed = [str(item).strip().lower() for item in runtime.outbound_allowlist_domains if str(item).strip()]
        if not allowed:
            return
        host = str(urlparse(url).hostname or "").strip().lower()
        if not host:
            raise GenerationError(f"Outbound URL missing host: {url}")
        for token in allowed:
            if _host_matches_allowlist(host, token):
                return
        raise GenerationError(f"Outbound URL blocked by allowlist: host={host}")


class SyntheticProvider(BaseProvider):
    """Offline synthetic generator used when API keys are unavailable."""

    name = "synthetic"

    def generate(self, prompt: str, negative_prompt: str, width: int, height: int, seed: int | None = None) -> Image.Image:
        del negative_prompt
        del seed
        prompt_lower = prompt.lower()
        image = Image.new("RGB", (width, height), (26, 39, 68))
        draw = ImageDraw.Draw(image, "RGBA")

        if any(token in prompt_lower for token in ("whale", "sea", "ocean", "ship", "ahab")):
            self._draw_whale_scene(draw, width, height)
        elif any(token in prompt_lower for token in ("dracula", "vampire", "castle", "gothic")):
            self._draw_gothic_scene(draw, width, height)
        elif any(token in prompt_lower for token in ("oil painting", "chiaroscuro", "dramatic")):
            self._draw_oil_scene(draw, width, height)
        else:
            self._draw_classical_scene(draw, width, height)

        self._overlay_engraving_texture(draw, width, height)
        return image

    @staticmethod
    def _draw_whale_scene(draw: ImageDraw.ImageDraw, width: int, height: int) -> None:
        draw.rectangle((0, int(height * 0.55), width, height), fill=(19, 65, 118, 220))

        for idx in range(14):
            y = int(height * 0.55 + idx * (height * 0.028))
            draw.arc(
                (-80, y - 26, width + 80, y + 26),
                0,
                180,
                fill=(120, 176, 219, 170),
                width=3,
            )

        draw.ellipse(
            (
                int(width * 0.18),
                int(height * 0.30),
                int(width * 0.82),
                int(height * 0.72),
            ),
            fill=(215, 221, 232, 235),
        )
        draw.polygon(
            [
                (int(width * 0.18), int(height * 0.52)),
                (int(width * 0.05), int(height * 0.60)),
                (int(width * 0.19), int(height * 0.64)),
            ],
            fill=(192, 203, 220, 225),
        )

        draw.polygon(
            [
                (int(width * 0.52), int(height * 0.73)),
                (int(width * 0.75), int(height * 0.73)),
                (int(width * 0.68), int(height * 0.82)),
                (int(width * 0.46), int(height * 0.82)),
            ],
            fill=(108, 78, 54, 240),
        )
        draw.line(
            (
                int(width * 0.60),
                int(height * 0.74),
                int(width * 0.60),
                int(height * 0.57),
            ),
            fill=(224, 198, 158, 230),
            width=4,
        )
        draw.polygon(
            [
                (int(width * 0.60), int(height * 0.58)),
                (int(width * 0.74), int(height * 0.66)),
                (int(width * 0.60), int(height * 0.66)),
            ],
            fill=(240, 231, 211, 195),
        )

    @staticmethod
    def _draw_gothic_scene(draw: ImageDraw.ImageDraw, width: int, height: int) -> None:
        draw.rectangle((0, 0, width, height), fill=(29, 22, 42, 220))
        draw.ellipse(
            (int(width * 0.62), int(height * 0.10), int(width * 0.90), int(height * 0.38)),
            fill=(176, 43, 59, 210),
        )
        draw.rectangle(
            (int(width * 0.22), int(height * 0.40), int(width * 0.52), int(height * 0.84)),
            fill=(17, 14, 25, 230),
        )
        draw.ellipse(
            (int(width * 0.58), int(height * 0.36), int(width * 0.82), int(height * 0.74)),
            fill=(38, 30, 45, 230),
        )

    @staticmethod
    def _draw_oil_scene(draw: ImageDraw.ImageDraw, width: int, height: int) -> None:
        draw.rectangle((0, 0, width, height), fill=(68, 54, 42, 210))
        draw.ellipse(
            (int(width * 0.10), int(height * 0.10), int(width * 0.46), int(height * 0.46)),
            fill=(248, 196, 112, 150),
        )
        draw.polygon(
            [
                (0, height),
                (int(width * 0.5), int(height * 0.58)),
                (width, height),
            ],
            fill=(22, 18, 26, 135),
        )
        draw.ellipse(
            (int(width * 0.32), int(height * 0.34), int(width * 0.70), int(height * 0.84)),
            fill=(125, 88, 68, 205),
        )

    @staticmethod
    def _draw_classical_scene(draw: ImageDraw.ImageDraw, width: int, height: int) -> None:
        draw.rectangle((0, 0, width, height), fill=(42, 53, 72, 210))
        draw.ellipse(
            (int(width * 0.15), int(height * 0.15), int(width * 0.85), int(height * 0.85)),
            fill=(146, 123, 90, 145),
        )
        draw.rectangle(
            (int(width * 0.25), int(height * 0.45), int(width * 0.75), int(height * 0.80)),
            fill=(92, 78, 62, 185),
        )

    @staticmethod
    def _overlay_engraving_texture(draw: ImageDraw.ImageDraw, width: int, height: int) -> None:
        step = max(6, width // 120)
        for y in range(0, height, step):
            draw.line((0, y, width, y + step // 2), fill=(233, 205, 158, 36), width=1)


class OpenAIProvider(BaseProvider):
    """OpenAI Images API."""

    name = "openai"

    def generate(self, prompt: str, negative_prompt: str, width: int, height: int, seed: int | None = None) -> Image.Image:
        if not self.api_key:
            raise GenerationError("Missing OPENAI_API_KEY")

        endpoint = "https://api.openai.com/v1/images/generations"
        self._assert_outbound_url(endpoint)
        seeded_prompt = f"{prompt}\nVariation seed: {seed}" if seed is not None else prompt
        payload = {
            "model": self.model,
            "prompt": f"{seeded_prompt}\nAvoid: {negative_prompt}",
            "size": f"{width}x{height}",
        }
        response = requests.post(
            endpoint,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=self.timeout,
        )
        if response.status_code in RETRYABLE_STATUS_CODES:
            raise RetryableGenerationError(
                f"OpenAI temporary error {response.status_code}: {response.text[:240]}",
                status_code=response.status_code,
            )
        if response.status_code >= 400:
            raise GenerationError(f"OpenAI error {response.status_code}: {response.text[:300]}")

        body = response.json()
        candidate = (body.get("data") or [{}])[0]
        if isinstance(candidate, dict):
            encoded = candidate.get("b64_json")
            if encoded:
                image_bytes = base64.b64decode(encoded)
                return Image.open(io.BytesIO(image_bytes)).convert("RGB")
            image_url = candidate.get("url")
            if image_url:
                return _download_image(str(image_url), timeout=self.timeout)
        raise GenerationError("OpenAI response missing image payload")


class OpenRouterProvider(BaseProvider):
    """OpenRouter image endpoint (OpenAI-compatible schema)."""

    name = "openrouter"

    def generate(self, prompt: str, negative_prompt: str, width: int, height: int, seed: int | None = None) -> Image.Image:
        if not self.api_key:
            raise GenerationError("Missing OPENROUTER_API_KEY")

        endpoint = "https://openrouter.ai/api/v1/chat/completions"
        self._assert_outbound_url(endpoint)
        seeded_prompt = f"{prompt}\nVariation seed: {seed}" if seed is not None else prompt
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": (
                        "Create a distinctly different artistic interpretation than prior variants. "
                        f"{seeded_prompt}\nAvoid: {negative_prompt}\nTarget size: {width}x{height}."
                    ),
                }
            ],
            "modalities": ["image"],
            "stream": False,
        }
        response = requests.post(
            endpoint,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://alexandria-cover-designer.local",
                "X-Title": "Alexandria Cover Designer",
            },
            json=payload,
            timeout=self.timeout,
        )
        if response.status_code in RETRYABLE_STATUS_CODES:
            raise RetryableGenerationError(
                f"OpenRouter temporary error {response.status_code}: {response.text[:240]}",
                status_code=response.status_code,
            )
        if response.status_code >= 400:
            raise GenerationError(f"OpenRouter error {response.status_code}: {response.text[:300]}")

        body = response.json()
        choices = body.get("choices") or []
        if choices:
            message = choices[0].get("message", {})
            images = message.get("images") or []
            for image_row in images:
                image_url = ""
                if isinstance(image_row, dict):
                    image_ref = image_row.get("image_url")
                    if isinstance(image_ref, dict):
                        image_url = str(image_ref.get("url", ""))
                    elif isinstance(image_ref, str):
                        image_url = image_ref
                if not image_url:
                    continue
                if image_url.startswith("data:image") and "," in image_url:
                    encoded = image_url.split(",", 1)[1]
                    image_bytes = base64.b64decode(encoded)
                    return Image.open(io.BytesIO(image_bytes)).convert("RGB")
                if image_url.startswith("http"):
                    return _download_image(image_url, timeout=self.timeout)

        # Backward-compatible fallback (older OpenAI-compatible image schema).
        candidate = (body.get("data") or [{}])[0]
        if isinstance(candidate, dict):
            if candidate.get("b64_json"):
                image_bytes = base64.b64decode(candidate["b64_json"])
                return Image.open(io.BytesIO(image_bytes)).convert("RGB")
            if candidate.get("url"):
                return _download_image(candidate["url"], timeout=self.timeout)

        raise GenerationError("OpenRouter response missing image payload")


class FalProvider(BaseProvider):
    """fal.ai generation endpoint."""

    name = "fal"

    def generate(self, prompt: str, negative_prompt: str, width: int, height: int, seed: int | None = None) -> Image.Image:
        if not self.api_key:
            raise GenerationError("Missing FAL_API_KEY")

        endpoint_model = self.model.replace("fal/", "")
        endpoint = f"https://fal.run/{endpoint_model}"
        self._assert_outbound_url(endpoint)
        payload: dict[str, Any] = {
            "prompt": prompt,
            "negative_prompt": negative_prompt,
            "image_size": {"width": width, "height": height},
        }
        if seed is not None:
            payload["seed"] = int(seed)
        response = requests.post(
            endpoint,
            headers={
                "Authorization": f"Key {self.api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=self.timeout,
        )
        if response.status_code in RETRYABLE_STATUS_CODES:
            raise RetryableGenerationError(
                f"fal.ai temporary error {response.status_code}: {response.text[:240]}",
                status_code=response.status_code,
            )
        if response.status_code >= 400:
            raise GenerationError(f"fal.ai error {response.status_code}: {response.text[:300]}")

        body = response.json()
        images = body.get("images") or body.get("output", {}).get("images") or []
        if not images:
            raise GenerationError("fal.ai response missing images")
        first = images[0]
        if isinstance(first, dict):
            url = first.get("url")
        else:
            url = str(first)
        if not url:
            raise GenerationError("fal.ai response image URL missing")
        return _download_image(url, timeout=self.timeout)


class ReplicateProvider(BaseProvider):
    """Replicate Predictions API."""

    name = "replicate"

    def generate(self, prompt: str, negative_prompt: str, width: int, height: int, seed: int | None = None) -> Image.Image:
        if not self.api_key:
            raise GenerationError("Missing REPLICATE_API_TOKEN")

        endpoint = "https://api.replicate.com/v1/predictions"
        self._assert_outbound_url(endpoint)
        input_payload: dict[str, Any] = {
            "prompt": prompt,
            "negative_prompt": negative_prompt,
            "width": width,
            "height": height,
        }
        if seed is not None:
            input_payload["seed"] = int(seed)
        create_response = requests.post(
            endpoint,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "version": self.model,
                "input": input_payload,
            },
            timeout=self.timeout,
        )

        if create_response.status_code in RETRYABLE_STATUS_CODES:
            raise RetryableGenerationError(
                f"Replicate temporary error {create_response.status_code}: {create_response.text[:240]}",
                status_code=create_response.status_code,
            )
        if create_response.status_code >= 400:
            raise GenerationError(
                f"Replicate error {create_response.status_code}: {create_response.text[:300]}"
            )

        prediction = create_response.json()
        prediction_id = prediction.get("id")
        if not prediction_id:
            raise GenerationError("Replicate response missing prediction id")

        poll_url = f"https://api.replicate.com/v1/predictions/{prediction_id}"
        self._assert_outbound_url(poll_url)
        deadline = time.time() + self.timeout
        while time.time() < deadline:
            poll = requests.get(
                poll_url,
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=self.timeout,
            )
            if poll.status_code in RETRYABLE_STATUS_CODES:
                time.sleep(1.0)
                continue
            if poll.status_code >= 400:
                raise GenerationError(f"Replicate poll error {poll.status_code}: {poll.text[:300]}")

            body = poll.json()
            status = body.get("status")
            if status == "succeeded":
                output = body.get("output")
                if isinstance(output, list) and output:
                    first = output[0]
                    if isinstance(first, dict):
                        output_url = first.get("url")
                        if output_url:
                            return _download_image(str(output_url), timeout=self.timeout)
                    return _download_image(str(first), timeout=self.timeout)
                if isinstance(output, str):
                    return _download_image(output, timeout=self.timeout)
                raise GenerationError("Replicate succeeded but output is empty")
            if status in {"failed", "canceled"}:
                raise GenerationError(f"Replicate prediction {status}: {body.get('error', 'unknown error')}")
            time.sleep(1.0)

        raise RetryableGenerationError("Replicate timed out while polling")


class GoogleCloudProvider(BaseProvider):
    """Google Generative API image endpoint (API key flow)."""

    name = "google"

    def generate(self, prompt: str, negative_prompt: str, width: int, height: int, seed: int | None = None) -> Image.Image:
        if not self.api_key:
            raise GenerationError("Missing GOOGLE_API_KEY")

        model_name = self.model if self.model.startswith("models/") else f"models/{self.model}"
        url = f"https://generativelanguage.googleapis.com/v1beta/{model_name}:generateContent"
        self._assert_outbound_url(url)
        prompt_text = (
            "Create a distinctly different artistic interpretation than prior variants. "
            f"{prompt}. Variation seed: {seed if seed is not None else 'n/a'}. "
            f"Avoid: {negative_prompt}"
        )
        payload = {
            "contents": [{"parts": [{"text": prompt_text}]}],
            "generationConfig": {
                "responseModalities": ["IMAGE"],
                "imageConfig": {"width": width, "height": height},
            },
        }
        response = requests.post(
            url,
            headers={"x-goog-api-key": self.api_key, "Content-Type": "application/json"},
            json=payload,
            timeout=self.timeout,
        )

        # Some models reject width/height imageConfig; retry once with modality-only config.
        if response.status_code == 400:
            fallback_payload = {
                "contents": [{"parts": [{"text": prompt_text}]}],
                "generationConfig": {
                    "responseModalities": ["IMAGE"],
                },
            }
            response = requests.post(
                url,
                headers={"x-goog-api-key": self.api_key, "Content-Type": "application/json"},
                json=fallback_payload,
                timeout=self.timeout,
            )

        if response.status_code in RETRYABLE_STATUS_CODES:
            raise RetryableGenerationError(
                f"Google temporary error {response.status_code}: {response.text[:240]}",
                status_code=response.status_code,
            )
        if response.status_code >= 400:
            raise GenerationError(f"Google error {response.status_code}: {response.text[:300]}")

        body = response.json()
        candidates = body.get("candidates", [])
        for candidate in candidates:
            parts = candidate.get("content", {}).get("parts", [])
            for part in parts:
                inline = part.get("inlineData", {}) or part.get("inline_data", {})
                data = inline.get("data")
                if data:
                    image_bytes = base64.b64decode(data)
                    return Image.open(io.BytesIO(image_bytes)).convert("RGB")

        generated_images = body.get("generatedImages", []) or body.get("generated_images", [])
        for item in generated_images:
            encoded = item.get("image", {}).get("imageBytes") if isinstance(item, dict) else None
            if encoded:
                image_bytes = base64.b64decode(encoded)
                return Image.open(io.BytesIO(image_bytes)).convert("RGB")

        raise GenerationError("Google response missing image bytes")


_PROVIDER_CLASS_MAP = {
    "openrouter": OpenRouterProvider,
    "fal": FalProvider,
    "replicate": ReplicateProvider,
    "openai": OpenAIProvider,
    "google": GoogleCloudProvider,
}


class ProviderRateLimiter:
    """Sliding-window limiter with per-second and per-minute caps."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._second_windows: dict[str, deque[float]] = defaultdict(deque)
        self._minute_windows: dict[str, deque[float]] = defaultdict(deque)

    def wait(self, provider: str, *, per_second: int, per_minute: int, base_delay: float) -> None:
        if base_delay > 0:
            time.sleep(base_delay)

        backoff = 1.0
        while True:
            now = time.monotonic()
            with self._lock:
                second_window = self._second_windows[provider]
                minute_window = self._minute_windows[provider]

                while second_window and (now - second_window[0]) >= 1.0:
                    second_window.popleft()
                while minute_window and (now - minute_window[0]) >= 60.0:
                    minute_window.popleft()

                sec_blocked = per_second > 0 and len(second_window) >= per_second
                min_blocked = per_minute > 0 and len(minute_window) >= per_minute
                if not sec_blocked and not min_blocked:
                    second_window.append(now)
                    minute_window.append(now)
                    return

            sleep_for = min(60.0, backoff)
            logger.warning(
                "Rate limit reached for provider '%s'; backing off %.1fs",
                provider,
                sleep_for,
            )
            time.sleep(sleep_for)
            backoff = min(60.0, backoff * 2.0)

    def reset(self, provider: str | None = None) -> None:
        with self._lock:
            if provider is None:
                self._second_windows.clear()
                self._minute_windows.clear()
                return
            token = str(provider).strip().lower()
            self._second_windows.pop(token, None)
            self._minute_windows.pop(token, None)

    def snapshot(self) -> dict[str, dict[str, int]]:
        now = time.monotonic()
        with self._lock:
            rows: dict[str, dict[str, int]] = {}
            providers = set(self._second_windows.keys()) | set(self._minute_windows.keys())
            for provider in providers:
                second_window = self._second_windows[provider]
                minute_window = self._minute_windows[provider]
                while second_window and (now - second_window[0]) >= 1.0:
                    second_window.popleft()
                while minute_window and (now - minute_window[0]) >= 60.0:
                    minute_window.popleft()
                rows[provider] = {
                    "rate_limit_window_second": len(second_window),
                    "rate_limit_window_minute": len(minute_window),
                }
            return rows


class ProviderCircuitBreaker:
    """Simple per-provider circuit breaker to avoid retry storms."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "state": "closed",
                "consecutive_failures": 0,
                "opened_until_monotonic": 0.0,
                "opened_until_utc": "",
                "last_error": "",
                "open_events": 0,
                "probe_in_flight": False,
            }
        )

    def allow(self, provider: str) -> tuple[bool, float]:
        now_mono = time.monotonic()
        with self._lock:
            state = self._state[provider]
            current_state = str(state.get("state", "closed") or "closed")
            opened_until = float(state.get("opened_until_monotonic", 0.0) or 0.0)

            if current_state == "open":
                if opened_until > now_mono:
                    return False, max(0.0, opened_until - now_mono)
                # Cooldown elapsed: permit exactly one half-open probe request.
                state["state"] = "half_open"
                state["opened_until_monotonic"] = 0.0
                state["opened_until_utc"] = ""
                state["consecutive_failures"] = 0
                state["probe_in_flight"] = False
                current_state = "half_open"

            if current_state == "half_open":
                if bool(state.get("probe_in_flight", False)):
                    return False, 0.25
                state["probe_in_flight"] = True
                return True, 0.0

            return True, 0.0

    def record_success(self, provider: str) -> None:
        with self._lock:
            state = self._state[provider]
            state["state"] = "closed"
            state["consecutive_failures"] = 0
            state["opened_until_monotonic"] = 0.0
            state["opened_until_utc"] = ""
            state["last_error"] = ""
            state["probe_in_flight"] = False

    def record_failure(
        self,
        provider: str,
        *,
        error_text: str,
        failure_threshold: int,
        cooldown_seconds: float,
        transient: bool = True,
    ) -> None:
        threshold = max(1, int(failure_threshold))
        cooldown = max(1.0, float(cooldown_seconds))
        now_mono = time.monotonic()
        now_utc = datetime.now(timezone.utc)
        with self._lock:
            state = self._state[provider]
            state["last_error"] = str(error_text or "")
            current_state = str(state.get("state", "closed") or "closed")

            if not transient:
                state["probe_in_flight"] = False
                if current_state == "half_open":
                    state["state"] = "closed"
                    state["consecutive_failures"] = 0
                return

            if current_state == "half_open":
                next_open_event = int(state.get("open_events", 0) or 0) + 1
                factor = min(4.0, float(2 ** max(0, next_open_event - 1)))
                adaptive_cooldown = min(900.0, cooldown * factor)
                state["state"] = "open"
                state["probe_in_flight"] = False
                state["consecutive_failures"] = threshold
                state["opened_until_monotonic"] = now_mono + adaptive_cooldown
                state["opened_until_utc"] = (now_utc + timedelta(seconds=adaptive_cooldown)).isoformat()
                state["open_events"] = next_open_event
                return

            state["consecutive_failures"] = int(state.get("consecutive_failures", 0) or 0) + 1
            if state["consecutive_failures"] < threshold:
                return
            next_open_event = int(state.get("open_events", 0) or 0) + 1
            factor = min(4.0, float(2 ** max(0, next_open_event - 1)))
            adaptive_cooldown = min(900.0, cooldown * factor)
            state["state"] = "open"
            state["probe_in_flight"] = False
            state["opened_until_monotonic"] = now_mono + adaptive_cooldown
            state["opened_until_utc"] = (now_utc + timedelta(seconds=adaptive_cooldown)).isoformat()
            state["open_events"] = next_open_event

    def snapshot(self) -> dict[str, dict[str, Any]]:
        now_mono = time.monotonic()
        with self._lock:
            rows: dict[str, dict[str, Any]] = {}
            for provider, values in self._state.items():
                opened_until = float(values.get("opened_until_monotonic", 0.0) or 0.0)
                remaining = max(0.0, opened_until - now_mono) if opened_until > 0 else 0.0
                state = str(values.get("state", "closed"))
                if state == "open" and opened_until <= now_mono:
                    state = "closed"
                rows[provider] = {
                    "state": state,
                    "consecutive_failures": int(values.get("consecutive_failures", 0) or 0),
                    "open_events": int(values.get("open_events", 0) or 0),
                    "cooldown_remaining_seconds": round(remaining, 3),
                    "opened_until_utc": str(values.get("opened_until_utc", "") or ""),
                    "last_error": str(values.get("last_error", "") or ""),
                    "probe_in_flight": bool(values.get("probe_in_flight", False)),
                }
            return rows

    def reset(self, provider: str | None = None) -> None:
        with self._lock:
            if provider is None:
                self._state.clear()
                return
            token = str(provider).strip().lower()
            self._state.pop(token, None)


_RATE_LIMITER = ProviderRateLimiter()
_CIRCUIT_BREAKER = ProviderCircuitBreaker()
_PROVIDER_STATS_LOCK = threading.Lock()
_PROVIDER_STATS: dict[str, dict[str, int]] = defaultdict(lambda: {"requests_today": 0, "errors_today": 0})


def _record_provider_request(provider: str, *, success: bool) -> None:
    with _PROVIDER_STATS_LOCK:
        stats = _PROVIDER_STATS[provider]
        stats["requests_today"] += 1
        if not success:
            stats["errors_today"] += 1


def _is_transient_provider_exception(exc: Exception) -> bool:
    if isinstance(exc, (RetryableGenerationError, requests.RequestException, TimeoutError)):
        return True
    status_code = getattr(exc, "status_code", None)
    return isinstance(status_code, int) and status_code in RETRYABLE_STATUS_CODES


def reset_provider_runtime_state(provider: str | None = None) -> None:
    """Reset in-memory provider runtime state (rate limiter, breaker, stats)."""
    token = str(provider).strip().lower() if provider else None
    _RATE_LIMITER.reset(token)
    _CIRCUIT_BREAKER.reset(token)
    with _PROVIDER_STATS_LOCK:
        if token is None:
            _PROVIDER_STATS.clear()
        else:
            _PROVIDER_STATS.pop(token, None)


def get_provider_runtime_stats() -> dict[str, dict[str, Any]]:
    breaker_state = _CIRCUIT_BREAKER.snapshot()
    limiter_state = _RATE_LIMITER.snapshot()
    with _PROVIDER_STATS_LOCK:
        rows: dict[str, dict[str, Any]] = {}
        for provider, values in _PROVIDER_STATS.items():
            merged: dict[str, Any] = values.copy()
            merged.update(breaker_state.get(provider, {}))
            merged.update(limiter_state.get(provider, {}))
            rows[provider] = merged
        for provider, values in breaker_state.items():
            if provider in rows:
                continue
            rows[provider] = {
                "requests_today": 0,
                "errors_today": 0,
                **values,
                **limiter_state.get(provider, {}),
            }
        for provider, values in limiter_state.items():
            if provider in rows:
                continue
            rows[provider] = {
                "requests_today": 0,
                "errors_today": 0,
                "state": "closed",
                "consecutive_failures": 0,
                "open_events": 0,
                "cooldown_remaining_seconds": 0.0,
                "opened_until_utc": "",
                "last_error": "",
                "probe_in_flight": False,
                **values,
            }
        return rows


def generate_image(
    prompt: str,
    negative_prompt: str,
    model: str,
    params: dict[str, Any],
    *,
    seed: int | None = None,
) -> bytes:
    """Generate a single image via the specified model/provider."""
    runtime = config.get_config()

    model_prefix = _model_provider_prefix(runtime, model)
    provider = model_prefix or params.get("provider") or runtime.resolve_model_provider(model)
    provider = str(provider).lower()
    provider_model = _resolve_provider_model_name(provider=provider, model=model)
    width = int(params.get("width", runtime.image_width))
    height = int(params.get("height", runtime.image_height))

    allowed, cooldown_remaining = _CIRCUIT_BREAKER.allow(provider)
    if not allowed:
        raise RetryableGenerationError(
            f"Provider '{provider}' is in cooldown ({cooldown_remaining:.1f}s remaining)",
            status_code=503,
        )

    request_delay = float(params.get("request_delay", _provider_request_delay(runtime, provider)))
    per_second = int(runtime.provider_rate_limit_per_second.get(provider, 0))
    per_minute = int(runtime.provider_rate_limit_per_minute.get(provider, 0))
    _RATE_LIMITER.wait(provider, per_second=per_second, per_minute=per_minute, base_delay=request_delay)

    provider_instance = _create_provider_instance(
        runtime=runtime,
        provider=provider,
        model=provider_model,
        allow_synthetic_fallback=bool(params.get("allow_synthetic_fallback", not runtime.has_any_api_key())),
    )

    try:
        image = provider_instance.generate(
            prompt=prompt,
            negative_prompt=negative_prompt,
            width=width,
            height=height,
            seed=seed,
        )
        _record_provider_request(provider, success=True)
        _CIRCUIT_BREAKER.record_success(provider)
    except Exception as exc:
        _record_provider_request(provider, success=False)
        _CIRCUIT_BREAKER.record_failure(
            provider,
            error_text=str(exc),
            failure_threshold=runtime.provider_circuit_failure_threshold,
            cooldown_seconds=runtime.provider_circuit_cooldown_seconds,
            transient=_is_transient_provider_exception(exc),
        )
        raise

    processed = _post_process_image(image, width=width, height=height)
    if _is_blank_or_solid(processed):
        raise GenerationError("Generated image rejected by blank/solid-color quality check")

    buffer = io.BytesIO()
    processed.save(buffer, format="PNG")
    return buffer.getvalue()


def generate_all_models(
    book_number: int,
    prompt: str,
    negative_prompt: str,
    models: list[str],
    variants_per_model: int,
    output_dir: Path,
    *,
    resume: bool = True,
    dry_run: bool = False,
    provider_override: str | None = None,
) -> list[GenerationResult]:
    """Fire ALL models concurrently for the same prompt."""
    runtime = config.get_config()
    output_dir.mkdir(parents=True, exist_ok=True)

    if variants_per_model < 1:
        raise ValueError("variants_per_model must be >= 1")
    if not models:
        raise ValueError("models list cannot be empty")

    results: list[GenerationResult] = []
    failures: list[GenerationResult] = []
    dry_run_plan: list[dict[str, Any]] = []

    tasks: list[tuple[str, int, Path, str, str, int]] = []
    rng = random.SystemRandom()
    for model_index, model in enumerate(models):
        model_dir = output_dir / str(book_number) / _model_to_directory(model)
        model_dir.mkdir(parents=True, exist_ok=True)

        provider = provider_override or runtime.resolve_model_provider(model)
        provider = provider.lower()

        for variant in range(1, variants_per_model + 1):
            image_path = model_dir / f"variant_{variant}.png"
            diversified_prompt = _diversify_prompt_for_model_variant(
                prompt=prompt,
                model=model,
                provider=provider,
                variant=variant,
                model_index=model_index,
            )
            seed = _variant_seed(rng=rng, book_number=book_number, model=model, variant=variant)
            if resume and image_path.exists():
                logger.info(
                    'Skipping existing image for book %s model "%s" variant %s',
                    book_number,
                    model,
                    variant,
                )
                results.append(
                    GenerationResult(
                        book_number=book_number,
                        variant=variant,
                        prompt=diversified_prompt,
                        model=model,
                        image_path=image_path,
                        success=True,
                        error=None,
                        generation_time=0.0,
                        cost=0.0,
                        provider=provider,
                        skipped=True,
                        attempts=0,
                    )
                )
                continue

            if dry_run:
                dry_run_plan.append(
                    {
                        "book_number": book_number,
                        "model": model,
                        "provider": provider,
                        "variant": variant,
                        "prompt": diversified_prompt,
                        "negative_prompt": negative_prompt,
                        "output_path": str(image_path),
                        "estimated_cost": runtime.get_model_cost(model),
                        "seed": seed,
                    }
                )
                results.append(
                    GenerationResult(
                        book_number=book_number,
                        variant=variant,
                        prompt=diversified_prompt,
                        model=model,
                        image_path=None,
                        success=True,
                        error=None,
                        generation_time=0.0,
                        cost=runtime.get_model_cost(model),
                        provider=provider,
                        dry_run=True,
                        attempts=0,
                    )
                )
                continue

            tasks.append((model, variant, image_path, provider, diversified_prompt, seed))

    if dry_run:
        _append_generation_plan(runtime.generation_plan_path, dry_run_plan)
        return _sort_results(results)

    max_workers = min(len(tasks), max(len(models), runtime.batch_concurrency, 1)) if tasks else 1
    if tasks:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(
                    _generate_one,
                    book_number=book_number,
                    variant=variant,
                    prompt=variant_prompt,
                    negative_prompt=negative_prompt,
                    model=model,
                    provider=provider,
                    output_path=image_path,
                    resume=resume,
                    seed=seed,
                ): (model, variant)
                for model, variant, image_path, provider, variant_prompt, seed in tasks
            }

            for future in as_completed(future_map):
                result = future.result()
                results.append(result)
                if not result.success:
                    failures.append(result)

    if tasks and results:
        results = _regenerate_near_duplicate_variants(
            runtime=runtime,
            book_number=book_number,
            negative_prompt=negative_prompt,
            output_dir=output_dir,
            results=results,
            resume=resume,
            provider_override=provider_override,
        )
        failures = [row for row in results if not row.success]

    if failures:
        _append_failures(runtime.failures_path, failures)

    return _sort_results(results)


def _diversify_prompt_for_variant(*, prompt: str, variant: int) -> str:
    base = prompt_generator.enforce_prompt_constraints(str(prompt or "").strip())
    diversified = prompt_generator.diversify_prompt(base, int(variant))
    if int(variant) > 1:
        diversified = (
            f"{diversified} Create a visibly distinct composition from prior variants for this title."
        ).strip()
    return prompt_generator.enforce_prompt_constraints(diversified)


def _stable_model_seed(*, model: str, provider: str) -> int:
    token = f"{provider.strip().lower()}::{model.strip().lower()}"
    digest = hashlib.sha1(token.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def _provider_model_hint(*, model: str, provider: str) -> str:
    token = f"{provider.strip().lower()} {model.strip().lower()}"
    for marker, hint in MODEL_PROVIDER_HINTS:
        if marker in token:
            return hint
    return ""


def _diversify_prompt_for_model_variant(
    *,
    prompt: str,
    model: str,
    provider: str,
    variant: int,
    model_index: int,
) -> str:
    diversified = _diversify_prompt_for_variant(prompt=prompt, variant=variant)
    if not MODEL_STYLE_PROFILES:
        return diversified

    stable_seed = _stable_model_seed(model=model, provider=provider)
    profile = MODEL_STYLE_PROFILES[(stable_seed + int(model_index) + int(variant)) % len(MODEL_STYLE_PROFILES)]
    provider_hint = _provider_model_hint(model=model, provider=provider)
    directive_parts = [
        f"Model signature: {provider.strip().lower()}/{model.strip().lower()}.",
        f"Style direction: {profile['style']}.",
        f"Color direction: {profile['palette']}.",
        f"Composition direction: {profile['composition']}.",
        f"Visual treatment: {profile['detail']}.",
        "Ensure this result is intentionally different from other models in the same run.",
    ]
    if provider_hint:
        directive_parts.append(provider_hint)
    merged = f"{' '.join(directive_parts)} {diversified}".strip()
    return prompt_generator.enforce_prompt_constraints(merged)


def _variant_seed(*, rng: random.Random | random.SystemRandom, book_number: int, model: str, variant: int) -> int:
    del book_number
    del model
    del variant
    return int(rng.getrandbits(32))


def _duplicate_prompt_suffix(*, variant: int, distance: float) -> str:
    return (
        "Force a substantially different visual outcome than previous variants. "
        f"Use a fresh palette/composition strategy for variant {int(variant)} (similarity distance={distance:.3f})."
    )


def _regenerate_near_duplicate_variants(
    *,
    runtime: config.Config,
    book_number: int,
    negative_prompt: str,
    output_dir: Path,
    results: list[GenerationResult],
    resume: bool,
    provider_override: str | None,
) -> list[GenerationResult]:
    del output_dir
    del resume
    distance_threshold = 0.15  # ~= 85%+ similar by inverse distance interpretation.
    viable: list[tuple[int, GenerationResult]] = []
    for idx, row in enumerate(results):
        if not row.success or row.dry_run or row.skipped or not row.image_path or not row.image_path.exists():
            continue
        viable.append((idx, row))
    if len(viable) < 2:
        return results

    try:
        regions = safe_json.load_json(
            config.cover_regions_path(catalog_id=runtime.catalog_id, config_dir=runtime.config_dir),
            {},
        )
    except Exception as exc:
        logger.debug("Similarity dedupe skipped (regions unavailable): %s", exc)
        return results

    grouped: dict[str, list[tuple[int, GenerationResult, Any]]] = {}
    for idx, row in viable:
        try:
            hash_obj = similarity_detector._compute_hash_for_book(  # type: ignore[attr-defined]
                book_number=book_number,
                image_path=row.image_path,
                regions=regions,
            )
        except Exception as exc:
            logger.debug("Similarity hash failed for %s: %s", row.image_path, exc)
            continue
        grouped.setdefault(row.model, []).append((idx, row, hash_obj))

    duplicate_targets: dict[int, tuple[GenerationResult, float]] = {}
    for model, rows in grouped.items():
        if len(rows) < 2:
            continue
        rows = sorted(rows, key=lambda item: item[1].variant)
        for i in range(len(rows)):
            for j in range(i + 1, len(rows)):
                left = rows[i]
                right = rows[j]
                try:
                    metrics = similarity_detector._compare_hash_objects(left[2], right[2])  # type: ignore[attr-defined]
                    distance = float(metrics.get("combined_similarity", 1.0) or 1.0)
                except Exception as exc:
                    logger.debug("Similarity compare failed for model %s: %s", model, exc)
                    continue
                if distance > distance_threshold:
                    continue
                idx = int(right[0])
                existing = duplicate_targets.get(idx)
                if existing is None or distance < existing[1]:
                    duplicate_targets[idx] = (right[1], distance)

    if not duplicate_targets:
        return results

    logger.warning(
        "Detected %d near-duplicate variant(s) for book %s; attempting one regeneration pass",
        len(duplicate_targets),
        book_number,
    )
    regen_rng = random.SystemRandom()
    for idx, (row, distance) in sorted(duplicate_targets.items(), key=lambda item: item[0]):
        if not row.image_path:
            continue
        regen_prompt = f"{row.prompt} {_duplicate_prompt_suffix(variant=row.variant, distance=distance)}".strip()
        regen_prompt = prompt_generator.enforce_prompt_constraints(regen_prompt)
        seed = _variant_seed(rng=regen_rng, book_number=book_number, model=row.model, variant=row.variant)
        provider = provider_override or row.provider or runtime.resolve_model_provider(row.model)
        regenerated = _generate_one(
            book_number=book_number,
            variant=row.variant,
            prompt=regen_prompt,
            negative_prompt=negative_prompt,
            model=row.model,
            provider=provider,
            output_path=row.image_path,
            resume=False,
            seed=seed,
        )
        if regenerated.success:
            logger.info(
                "Regenerated near-duplicate variant for book %s model %s variant %s (distance %.3f)",
                book_number,
                row.model,
                row.variant,
                distance,
            )
            results[idx] = regenerated
        else:
            logger.warning(
                "Failed to regenerate near-duplicate variant for book %s model %s variant %s: %s",
                book_number,
                row.model,
                row.variant,
                regenerated.error,
            )

    return results


def generate_single_book(
    book_number: int,
    prompts_path: Path,
    output_dir: Path,
    models: list[str] | None = None,
    variants: int = 5,
    *,
    prompt_variant: int = 1,
    prompt_text: str | None = None,
    negative_prompt: str | None = None,
    provider_override: str | None = None,
    library_prompt_id: str | None = None,
    resume: bool = True,
    dry_run: bool = False,
) -> list[GenerationResult]:
    """Primary single-cover entry point for iterative generation (D19)."""
    runtime = config.get_config()

    payload = _load_prompts_payload(prompts_path)
    book_entry = _find_book_entry(payload, book_number)
    title = str(book_entry.get("title", f"Book {book_number}"))

    base_variant = _find_variant(book_entry, prompt_variant)
    selected_negative_prompt = negative_prompt or str(base_variant.get("negative_prompt", ""))

    selected_prompt = prompt_text
    if library_prompt_id:
        prompt_library = PromptLibrary(runtime.prompt_library_path)
        library_matches = [prompt for prompt in prompt_library.get_prompts() if prompt.id == library_prompt_id]
        if not library_matches:
            raise KeyError(f"Prompt id '{library_prompt_id}' not found in prompt library")
        selected_prompt = library_matches[0].prompt_template.format(title=title)
        if not negative_prompt:
            selected_negative_prompt = library_matches[0].negative_prompt

    if not selected_prompt:
        selected_prompt = str(base_variant.get("prompt", "")).strip()

    active_models = models[:] if models else runtime.all_models[:]
    if not active_models:
        active_models = [runtime.ai_model]

    logger.info(
        "Generating single book %s using %d model(s), %d variant(s)/model",
        book_number,
        len(active_models),
        variants,
    )

    return generate_all_models(
        book_number=book_number,
        prompt=selected_prompt,
        negative_prompt=selected_negative_prompt,
        models=active_models,
        variants_per_model=variants,
        output_dir=output_dir,
        resume=resume,
        dry_run=dry_run,
        provider_override=provider_override,
    )


def generate_batch(
    prompts_path: Path,
    output_dir: Path,
    resume: bool = True,
    *,
    books: list[int] | None = None,
    model: str | None = None,
    dry_run: bool = False,
    max_books: int = 20,
) -> list[GenerationResult]:
    """Batch generation mode for validated model/prompt combinations.

    D23 scope default: first 20 titles only.
    """
    runtime = config.get_config()
    payload = _load_prompts_payload(prompts_path)

    all_books = sorted(payload.get("books", []), key=lambda item: int(item.get("number", 0)))
    if books:
        wanted = {int(num) for num in books}
        all_books = [item for item in all_books if int(item.get("number", 0)) in wanted]
    else:
        all_books = all_books[:max_books]

    chosen_model = model or runtime.ai_model
    chosen_provider = runtime.resolve_model_provider(chosen_model)

    total_jobs = sum(min(runtime.variants_per_cover, len(entry.get("variants", []))) for entry in all_books)
    completed = 0

    results: list[GenerationResult] = []
    failures: list[GenerationResult] = []
    dry_run_plan: list[dict[str, Any]] = []

    for book_entry in all_books:
        book_number = int(book_entry.get("number", 0))
        title = str(book_entry.get("title", f"Book {book_number}"))
        variants = sorted(book_entry.get("variants", []), key=lambda item: int(item.get("variant_id", 0)))
        variants = variants[: runtime.variants_per_cover]

        for variant_entry in variants:
            completed += 1
            variant_id = int(variant_entry.get("variant_id", completed))
            prompt = str(variant_entry.get("prompt", ""))
            negative_prompt = str(variant_entry.get("negative_prompt", ""))
            image_path = output_dir / str(book_number) / f"variant_{variant_id}.png"

            if resume and image_path.exists():
                logger.info(
                    "[%d/%d] Skipping Variant %d for \"%s\" (already exists)",
                    completed,
                    total_jobs,
                    variant_id,
                    title,
                )
                results.append(
                    GenerationResult(
                        book_number=book_number,
                        variant=variant_id,
                        prompt=prompt,
                        model=chosen_model,
                        image_path=image_path,
                        success=True,
                        error=None,
                        generation_time=0.0,
                        cost=0.0,
                        provider=chosen_provider,
                        skipped=True,
                        attempts=0,
                    )
                )
                continue

            logger.info(
                "[%d/%d] Generating Variant %d for \"%s\"...",
                completed,
                total_jobs,
                variant_id,
                title,
            )

            if dry_run:
                dry_run_plan.append(
                    {
                        "book_number": book_number,
                        "model": chosen_model,
                        "provider": chosen_provider,
                        "variant": variant_id,
                        "prompt": prompt,
                        "negative_prompt": negative_prompt,
                        "output_path": str(image_path),
                        "estimated_cost": runtime.get_model_cost(chosen_model),
                    }
                )
                results.append(
                    GenerationResult(
                        book_number=book_number,
                        variant=variant_id,
                        prompt=prompt,
                        model=chosen_model,
                        image_path=None,
                        success=True,
                        error=None,
                        generation_time=0.0,
                        cost=runtime.get_model_cost(chosen_model),
                        provider=chosen_provider,
                        dry_run=True,
                        attempts=0,
                    )
                )
                continue

            result = _generate_one(
                book_number=book_number,
                variant=variant_id,
                prompt=prompt,
                negative_prompt=negative_prompt,
                model=chosen_model,
                provider=chosen_provider,
                output_path=image_path,
                resume=resume,
            )
            results.append(result)
            if not result.success:
                failures.append(result)

    if dry_run and dry_run_plan:
        _append_generation_plan(runtime.generation_plan_path, dry_run_plan)

    if failures:
        _append_failures(runtime.failures_path, failures)

    return _sort_results(results)


def _generate_one(
    *,
    book_number: int,
    variant: int,
    prompt: str,
    negative_prompt: str,
    model: str,
    provider: str,
    output_path: Path,
    resume: bool,
    seed: int | None = None,
) -> GenerationResult:
    runtime = config.get_config()
    catalog_id = getattr(runtime, "catalog_id", None)

    prompt_similarity_alert: str | None = None
    try:
        prompt_similarity = similarity_detector.check_prompt_similarity_against_winners(
            prompt=prompt,
            current_book=book_number,
            winner_selections_path=config.winner_selections_path(catalog_id=catalog_id, data_dir=runtime.data_dir),
            generation_history_path=config.generation_history_path(catalog_id=catalog_id, data_dir=runtime.data_dir),
            threshold=0.85,
        )
        if bool(prompt_similarity.get("alert")):
            close_book = prompt_similarity.get("closest_book")
            similarity_value = float(prompt_similarity.get("similarity", 0.0) or 0.0)
            prompt_similarity_alert = (
                f"Prompt similarity warning: book {book_number} prompt is {similarity_value:.3f} similar to winner prompt for book {close_book}."
            )
            logger.warning(prompt_similarity_alert)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Prompt similarity pre-check failed: %s", exc)

    if resume and output_path.exists():
        return GenerationResult(
            book_number=book_number,
            variant=variant,
            prompt=prompt,
            model=model,
            image_path=output_path,
            success=True,
            error=None,
            generation_time=0.0,
            cost=0.0,
            provider=provider,
            skipped=True,
            attempts=0,
        )

    start = time.perf_counter()
    last_error: str | None = None
    model_prefix = _model_provider_prefix(runtime, model)
    if model_prefix:
        provider_chain = [model_prefix]
    else:
        provider_chain = _provider_fallback_chain(runtime, primary=provider)
    provider_index = 0
    active_provider = provider_chain[provider_index]
    consecutive_provider_failures = 0

    attempt = 0
    max_attempts = max(1, runtime.max_retries) * max(1, len(provider_chain))
    while attempt < max_attempts:
        # Skip providers that are currently in cooldown.
        provider_advanced = False
        while provider_index < len(provider_chain):
            candidate = provider_chain[provider_index]
            allowed, cooldown_remaining = _CIRCUIT_BREAKER.allow(candidate)
            if allowed:
                active_provider = candidate
                break
            logger.warning(
                "Skipping provider '%s' for book %s model %s variant %s due to cooldown (%.1fs remaining)",
                candidate,
                book_number,
                model,
                variant,
                cooldown_remaining,
            )
            provider_index += 1
            provider_advanced = True
        if provider_index >= len(provider_chain):
            last_error = "All providers are in cooldown"
            break
        if provider_advanced:
            consecutive_provider_failures = 0

        attempt += 1
        try:
            image_bytes = generate_image(
                prompt=prompt,
                negative_prompt=negative_prompt,
                model=model,
                params={
                    "provider": active_provider,
                    "width": runtime.image_width,
                    "height": runtime.image_height,
                    "request_delay": _provider_request_delay(runtime, active_provider),
                    "allow_synthetic_fallback": not runtime.has_any_api_key(),
                },
                seed=seed,
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(image_bytes)

            similar_to_book: int | None = None
            distinctiveness_score: float = 1.0
            post_warning: str | None = prompt_similarity_alert
            try:
                post_check = similarity_detector.check_generated_image_against_winners(
                    image_path=output_path,
                    book_number=book_number,
                    output_dir=runtime.output_dir,
                    catalog_path=runtime.book_catalog_path,
                    winner_selections_path=config.winner_selections_path(catalog_id=catalog_id, data_dir=runtime.data_dir),
                    regions_path=config.cover_regions_path(catalog_id=catalog_id, config_dir=runtime.config_dir),
                    threshold=0.25,
                )
                nearest_similarity = float(post_check.get("similarity", 1.0) or 1.0)
                distinctiveness_score = max(0.0, min(1.0, nearest_similarity))
                similar_to_book = post_check.get("closest_book")
                if bool(post_check.get("alert")) and similar_to_book:
                    suffix = f"SIMILAR TO BOOK #{similar_to_book} (distance={nearest_similarity:.3f})"
                    post_warning = f"{post_warning} | {suffix}" if post_warning else suffix
                    logger.warning(
                        "Post-generation similarity alert for book %s variant %s model %s: %s",
                        book_number,
                        variant,
                        model,
                        suffix,
                    )
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Post-generation similarity check failed: %s", exc)

            elapsed = time.perf_counter() - start
            return GenerationResult(
                book_number=book_number,
                variant=variant,
                prompt=prompt,
                model=model,
                image_path=output_path,
                success=True,
                error=None,
                generation_time=elapsed,
                cost=runtime.get_model_cost(model),
                provider=active_provider,
                attempts=attempt,
                similarity_warning=post_warning,
                similar_to_book=similar_to_book,
                distinctiveness_score=distinctiveness_score,
            )
        except RetryableGenerationError as exc:
            last_error = str(exc)
            consecutive_provider_failures += 1
            if consecutive_provider_failures >= 3 and provider_index < (len(provider_chain) - 1):
                previous_provider = active_provider
                provider_index += 1
                active_provider = provider_chain[provider_index]
                consecutive_provider_failures = 0
                logger.warning(
                    "Provider failover triggered for book %s model %s variant %s: %s -> %s",
                    book_number,
                    model,
                    variant,
                    previous_provider,
                    active_provider,
                )
                continue
            if attempt >= max_attempts:
                break
            if "in cooldown" in str(exc).lower() and provider_index < (len(provider_chain) - 1):
                previous_provider = active_provider
                provider_index += 1
                active_provider = provider_chain[provider_index]
                consecutive_provider_failures = 0
                logger.warning(
                    "Provider cooldown failover for book %s model %s variant %s: %s -> %s",
                    book_number,
                    model,
                    variant,
                    previous_provider,
                    active_provider,
                )
                continue
            backoff = min(60.0, max(1.0, runtime.request_delay * (2 ** (attempt - 1))))
            logger.warning(
                "Retryable error for book %s model %s variant %s (%d/%d): %s",
                book_number,
                model,
                variant,
                attempt,
                max_attempts,
                exc,
            )
            time.sleep(backoff)
        except GenerationError as exc:
            last_error = str(exc)
            consecutive_provider_failures += 1
            if consecutive_provider_failures >= 3 and provider_index < (len(provider_chain) - 1):
                previous_provider = active_provider
                provider_index += 1
                active_provider = provider_chain[provider_index]
                consecutive_provider_failures = 0
                logger.warning(
                    "Provider failover triggered for book %s model %s variant %s after GenerationError: %s -> %s",
                    book_number,
                    model,
                    variant,
                    previous_provider,
                    active_provider,
                )
                continue
            if attempt >= max_attempts:
                break
        except requests.RequestException as exc:
            last_error = f"Request failure: {exc}"
            consecutive_provider_failures += 1
            if consecutive_provider_failures >= 3 and provider_index < (len(provider_chain) - 1):
                previous_provider = active_provider
                provider_index += 1
                active_provider = provider_chain[provider_index]
                consecutive_provider_failures = 0
                logger.warning(
                    "Provider failover triggered for book %s model %s variant %s after network failures: %s -> %s",
                    book_number,
                    model,
                    variant,
                    previous_provider,
                    active_provider,
                )
                continue
            if attempt >= max_attempts:
                break
            backoff = min(60.0, max(1.0, runtime.request_delay * (2 ** (attempt - 1))))
            logger.warning(
                "Network retry for book %s model %s variant %s (%d/%d): %s",
                book_number,
                model,
                variant,
                attempt,
                max_attempts,
                exc,
            )
            time.sleep(backoff)

    elapsed = time.perf_counter() - start
    return GenerationResult(
        book_number=book_number,
        variant=variant,
        prompt=prompt,
        model=model,
        image_path=None,
        success=False,
        error=last_error or "Unknown generation failure",
        generation_time=elapsed,
        cost=0.0,
        provider=active_provider,
        attempts=attempt,
    )


def _provider_request_delay(runtime: config.Config, provider: str) -> float:
    return float(runtime.provider_request_delay.get(provider, runtime.request_delay))


def _provider_fallback_chain(runtime: config.Config, *, primary: str) -> list[str]:
    primary_token = str(primary or "").strip().lower()
    any_key = runtime.has_any_api_key()

    def _provider_enabled(token: str) -> bool:
        if not any_key:
            return True
        return bool(runtime.get_api_key(token).strip())

    providers: list[str] = []
    if primary_token and _provider_enabled(primary_token):
        providers.append(primary_token)
    for provider in runtime.provider_keys.keys():
        token = str(provider).strip().lower()
        if not token or token in providers:
            continue
        if not _provider_enabled(token):
            continue
        providers.append(token)
    if not providers and primary_token:
        providers.append(primary_token)
    return providers


def _create_provider_instance(
    *,
    runtime: config.Config,
    provider: str,
    model: str,
    allow_synthetic_fallback: bool,
) -> BaseProvider:
    api_key = runtime.get_api_key(provider)

    if provider not in _PROVIDER_CLASS_MAP:
        raise GenerationError(f"Unsupported provider: {provider}")

    if not api_key and allow_synthetic_fallback:
        logger.info(
            "No API key configured for provider '%s'; using synthetic provider fallback for local iteration",
            provider,
        )
        return SyntheticProvider(model=model, runtime=runtime)

    provider_class = _PROVIDER_CLASS_MAP[provider]
    return provider_class(model=model, api_key=api_key, runtime=runtime)


def _resolve_provider_model_name(provider: str, model: str) -> str:
    """Strip provider prefix from provider/model notation."""
    token = model.strip()
    if "/" not in token:
        return token

    prefix, suffix = token.split("/", 1)
    if prefix.lower() == provider.lower() and suffix:
        return suffix
    return token


def _model_provider_prefix(runtime: config.Config, model: str) -> str | None:
    """Return explicit provider prefix for provider/model notation, when present."""
    token = str(model).strip()
    if "/" not in token:
        return None
    prefix = token.split("/", 1)[0].strip().lower()
    if prefix in runtime.provider_keys:
        return prefix
    return None


def _post_process_image(image: Image.Image, width: int, height: int) -> Image.Image:
    processed = image.convert("RGBA").resize((width, height), Image.LANCZOS)

    mask = Image.new("L", (width, height), 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse((0, 0, width - 1, height - 1), fill=255)
    processed.putalpha(mask)

    return processed


def _is_blank_or_solid(image: Image.Image) -> bool:
    rgb = np.array(image.convert("RGB"), dtype=np.uint8)
    std = float(rgb.std())
    min_val = int(rgb.min())
    max_val = int(rgb.max())
    unique_ratio = float(np.unique(rgb.reshape(-1, 3), axis=0).shape[0]) / float(rgb.shape[0] * rgb.shape[1])
    return std < 4.0 or (max_val - min_val) < 8 or unique_ratio < 0.00001


def _download_image(url: str, timeout: float = 120.0) -> Image.Image:
    response = requests.get(url, timeout=timeout)
    if response.status_code in RETRYABLE_STATUS_CODES:
        raise RetryableGenerationError(
            f"Temporary download error {response.status_code} for {url}",
            status_code=response.status_code,
        )
    if response.status_code >= 400:
        raise GenerationError(f"Image download failed {response.status_code}: {url}")
    return Image.open(io.BytesIO(response.content)).convert("RGB")


def _load_prompts_payload(prompts_path: Path) -> dict[str, Any]:
    payload = safe_json.load_json(prompts_path, {})
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid prompts file at {prompts_path}: expected object payload")
    books = payload.get("books")
    if not isinstance(books, list):
        raise ValueError(f"Invalid prompts file at {prompts_path}: missing 'books' list")
    return payload


def _find_book_entry(payload: dict[str, Any], book_number: int) -> dict[str, Any]:
    for book in payload.get("books", []):
        if int(book.get("number", 0)) == int(book_number):
            return book
    raise KeyError(f"Book #{book_number} not found in prompts file")


def _find_variant(book_entry: dict[str, Any], variant_id: int) -> dict[str, Any]:
    variants = book_entry.get("variants", [])
    for item in variants:
        if int(item.get("variant_id", 0)) == int(variant_id):
            return item
    if variants:
        return variants[0]
    raise KeyError(f"Book {book_entry.get('number')} has no variants")


def _model_to_directory(model: str) -> str:
    return model.strip().lower().replace("/", "__").replace(" ", "_")


def _error_kind(error_message: str | None) -> str:
    text = (error_message or "").lower()
    if "429" in text or "rate" in text:
        return "rate_limit"
    if "timeout" in text or "timed out" in text:
        return "timeout"
    if "key" in text or "credential" in text or "auth" in text:
        return "auth"
    return "provider_error"


def _append_failures(path: Path, failed_results: list[GenerationResult]) -> None:
    payload = safe_json.load_json(path, {})

    existing = payload.get("failures") if isinstance(payload, dict) else None
    if not isinstance(existing, list):
        existing = []

    timestamp = datetime.now(timezone.utc).isoformat()
    for result in failed_results:
        existing.append(
            {
                "timestamp": timestamp,
                "book_number": result.book_number,
                "variant": result.variant,
                "model": result.model,
                "provider": result.provider,
                "prompt": result.prompt,
                "error_kind": _error_kind(result.error),
                "error": result.error,
                "retries": result.attempts,
            }
        )

    output = {
        "updated_at": timestamp,
        "failures": existing,
    }
    safe_json.atomic_write_json(path, output)


def retry_failures(*, failures_path: Path, output_dir: Path, resume: bool = False) -> list[GenerationResult]:
    """Retry only failed generation rows from failure log."""
    payload = safe_json.load_json(failures_path, {})
    failures = payload.get("failures", []) if isinstance(payload, dict) else []
    if not isinstance(failures, list):
        failures = []

    results: list[GenerationResult] = []
    seen: set[tuple[int, int, str]] = set()
    for row in failures:
        if not isinstance(row, dict):
            continue
        book = _safe_int(row.get("book_number"), 0)
        variant = _safe_int(row.get("variant"), 0)
        model = str(row.get("model", ""))
        provider = str(row.get("provider", "")) or config.get_config().resolve_model_provider(model)
        prompt = str(row.get("prompt", ""))
        if book <= 0 or variant <= 0 or not model or not prompt:
            continue
        key = (book, variant, model)
        if key in seen:
            continue
        seen.add(key)
        output_path = output_dir / str(book) / _model_to_directory(model) / f"variant_{variant}.png"
        results.append(
            _generate_one(
                book_number=book,
                variant=variant,
                prompt=prompt,
                negative_prompt="",
                model=model,
                provider=provider,
                output_path=output_path,
                resume=resume,
            )
        )

    return _sort_results(results)


def _append_generation_plan(path: Path, plan_rows: list[dict[str, Any]]) -> None:
    if not plan_rows:
        return

    payload = safe_json.load_json(path, {})

    existing = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(existing, list):
        existing = []

    existing.extend(plan_rows)
    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "items": existing,
    }
    safe_json.atomic_write_json(path, output)


def _sort_results(results: list[GenerationResult]) -> list[GenerationResult]:
    return sorted(results, key=lambda item: (item.book_number, item.model, item.variant, item.image_path is None))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_books_arg(raw: str | None) -> list[int] | None:
    if not raw:
        return None

    result: set[int] = set()
    for part in raw.split(","):
        token = part.strip()
        if not token:
            continue

        if "-" in token:
            start_str, end_str = token.split("-", 1)
            start = int(start_str)
            end = int(end_str)
            for value in range(min(start, end), max(start, end) + 1):
                result.add(value)
        else:
            result.add(int(token))

    return sorted(result)


def _build_models_from_args(args: argparse.Namespace, runtime: config.Config) -> list[str] | None:
    if args.all_models:
        return runtime.all_models[:]
    if args.models:
        return [token.strip() for token in args.models.split(",") if token.strip()]
    if args.model:
        return [args.model.strip()]
    return None


def _summarize_results(results: list[GenerationResult]) -> dict[str, Any]:
    total = len(results)
    success = sum(1 for result in results if result.success)
    failed = sum(1 for result in results if not result.success)
    skipped = sum(1 for result in results if result.skipped)
    dry_run = sum(1 for result in results if result.dry_run)
    total_cost = sum(result.cost for result in results)

    return {
        "total": total,
        "success": success,
        "failed": failed,
        "skipped": skipped,
        "dry_run": dry_run,
        "total_cost": round(total_cost, 4),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Prompt 2A image generation pipeline")
    parser.add_argument("--prompts-path", type=Path, default=config.PROMPTS_PATH)
    parser.add_argument("--output-dir", type=Path, default=config.TMP_DIR / "generated")

    parser.add_argument("--book", type=int, help="Single book number for iteration mode")
    parser.add_argument("--books", type=str, help="Batch selection, e.g. 1-20 or 2,5,8")

    parser.add_argument("--model", type=str, help="Single model, e.g. openai/gpt-image-1")
    parser.add_argument("--models", type=str, help="Comma-separated model list")
    parser.add_argument("--all-models", action="store_true", help="Use all configured models")

    parser.add_argument("--variants", type=int, default=config.VARIANTS_PER_COVER)
    parser.add_argument("--prompt-variant", type=int, default=1)
    parser.add_argument("--prompt-text", type=str, default=None)
    parser.add_argument("--negative-prompt", type=str, default=None)
    parser.add_argument("--library-prompt-id", type=str, default=None)

    parser.add_argument("--provider", type=str, default=None, help="Override provider for all requests")
    parser.add_argument("--dry-run", action="store_true", help="Save generation plan without generating images")
    parser.add_argument("--no-resume", action="store_true", help="Disable skip-existing behavior")

    parser.add_argument(
        "--max-books",
        type=int,
        default=20,
        help="Batch scope limit (default 20 per D23)",
    )

    args = parser.parse_args()
    runtime = config.get_config()

    models = _build_models_from_args(args, runtime)
    resume = not args.no_resume

    if args.book is not None:
        results = generate_single_book(
            book_number=args.book,
            prompts_path=args.prompts_path,
            output_dir=args.output_dir,
            models=models,
            variants=args.variants,
            prompt_variant=args.prompt_variant,
            prompt_text=args.prompt_text,
            negative_prompt=args.negative_prompt,
            provider_override=args.provider,
            library_prompt_id=args.library_prompt_id,
            resume=resume,
            dry_run=args.dry_run,
        )
    else:
        book_selection = _parse_books_arg(args.books)
        chosen_model = None
        if models:
            chosen_model = models[0]

        results = generate_batch(
            prompts_path=args.prompts_path,
            output_dir=args.output_dir,
            resume=resume,
            books=book_selection,
            model=chosen_model,
            dry_run=args.dry_run,
            max_books=args.max_books,
        )

    summary = _summarize_results(results)
    logger.info("Generation summary: %s", summary)
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
