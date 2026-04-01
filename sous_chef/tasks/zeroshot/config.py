"""Defaults and environment for zero-shot classification."""
from __future__ import annotations

import os
from typing import Literal

DEFAULT_ZEROSHOT_MODEL = "MoritzLaurer/bge-m3-zeroshot-v2.0"

# Reserved top label when inference fails after retries (not counted toward user label distribution).
ZEROSHOT_UNKNOWN_LABEL = "unknown"

# Flow-owned defaults (not user parameters): MediaCloud stories use `text`; CPU unless you change this constant.
ZEROSHOT_STORY_TEXT_COLUMN = "text"
ZEROSHOT_CLASSIFY_DEVICE = -1
# Truncation before inference (task default; not a Kitchen param unless a flow exposes it).
ZEROSHOT_TEXT_MAX_CHARS_DEFAULT = 2000

# Hugging Face InferenceClient HTTP timeout (seconds). Larger than typical gateway
# idle limits so the client waits for slow zero-shot responses before InferenceTimeoutError.
ZEROSHOT_HF_INFERENCE_TIMEOUT_S = 180.0

ZEROSHOT_BACKEND_ENV = "ZEROSHOT_BACKEND"

# Default metadata columns for CSV export (never includes full story `text`).
ZEROSHOT_DEFAULT_STORY_COLUMNS: list[str] = [
    "story_id",
    "title",
    "url",
    "publish_date",
    "media_name",
    "language",
]

ZeroshotBackend = Literal["local", "hf_inference"]


def get_zeroshot_backend(explicit: str | None = None) -> ZeroshotBackend:
    """Resolve backend: explicit arg, then ``ZEROSHOT_BACKEND`` env, default ``hf_inference``."""
    raw = (explicit or os.environ.get(ZEROSHOT_BACKEND_ENV) or "hf_inference").strip().lower()
    if raw in ("local",):
        return "local"
    if raw in ("hf_inference", "hf", "hosted", "inference_api"):
        return "hf_inference"
    raise ValueError(
        f"Invalid {ZEROSHOT_BACKEND_ENV}={raw!r}; use 'local' or 'hf_inference'"
    )
