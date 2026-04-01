"""
Zero-shot text classification (local ``transformers`` or Hugging Face Inference API).

Public entry point for flows: :func:`add_zero_shot_classification` respects
``ZEROSHOT_BACKEND`` (``local`` | ``hf_inference``).
"""
from __future__ import annotations

from .classify import add_zero_shot_classification
from .common import (
    _truncate,
    compute_zero_shot_label_counts,
    story_dataframe_for_zeroshot_csv,
    zeroshot_classification_failure_details,
)
from .config import (
    DEFAULT_ZEROSHOT_MODEL,
    ZEROSHOT_BACKEND_ENV,
    ZEROSHOT_CLASSIFY_DEVICE,
    ZEROSHOT_DEFAULT_STORY_COLUMNS,
    ZEROSHOT_STORY_TEXT_COLUMN,
    ZEROSHOT_TEXT_MAX_CHARS_DEFAULT,
    get_zeroshot_backend,
)

__all__ = [
    "DEFAULT_ZEROSHOT_MODEL",
    "ZEROSHOT_BACKEND_ENV",
    "ZEROSHOT_CLASSIFY_DEVICE",
    "ZEROSHOT_DEFAULT_STORY_COLUMNS",
    "ZEROSHOT_STORY_TEXT_COLUMN",
    "ZEROSHOT_TEXT_MAX_CHARS_DEFAULT",
    "_truncate",
    "add_zero_shot_classification",
    "compute_zero_shot_label_counts",
    "get_zeroshot_backend",
    "story_dataframe_for_zeroshot_csv",
    "zeroshot_classification_failure_details",
]
