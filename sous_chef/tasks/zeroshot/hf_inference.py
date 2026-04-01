"""Hugging Face Inference API (hosted) zero-shot classification."""
from __future__ import annotations

import json
import logging
import time
from typing import List, Optional

import pandas as pd
from huggingface_hub import InferenceClient
from huggingface_hub.errors import HfHubHTTPError, InferenceTimeoutError

from sous_chef.secrets import get_hf_bill_to, get_llm_api_key

from .common import _append_passing_threshold_column, _truncate
from .config import (
    DEFAULT_ZEROSHOT_MODEL,
    ZEROSHOT_HF_INFERENCE_TIMEOUT_S,
    ZEROSHOT_TEXT_MAX_CHARS_DEFAULT,
    ZEROSHOT_UNKNOWN_LABEL,
)

# HTTP statuses treated as transient for hosted inference (gateway / overload / cold model).
_RETRYABLE_HF_HTTP_STATUSES = frozenset({502, 503, 504})

logger = logging.getLogger(__name__)


def _hf_token_optional() -> Optional[str]:
    try:
        return get_llm_api_key("huggingface")
    except ValueError:
        return None


def _classify_one_hf(
    client: InferenceClient,
    model: str,
    text: str,
    candidate_labels: List[str],
    hypothesis_template: str,
    multi_label: bool,
) -> tuple[list[str], list[float]]:
    if not text.strip():
        return [], []
    elements = client.zero_shot_classification(
        text,
        candidate_labels=candidate_labels,
        multi_label=multi_label,
        hypothesis_template=hypothesis_template,
        model=model,
    )
    labels = [e.label for e in elements]
    scores = [float(e.score) for e in elements]
    return labels, scores


def _call_with_model_loading_retry(
    client: InferenceClient,
    model: str,
    text: str,
    candidate_labels: List[str],
    hypothesis_template: str,
    multi_label: bool,
    *,
    max_retries: int = 6,
    base_delay_s: float = 3.0,
) -> tuple[list[str], list[float]]:
    for attempt in range(max_retries):
        try:
            return _classify_one_hf(
                client,
                model,
                text,
                candidate_labels,
                hypothesis_template,
                multi_label,
            )
        except InferenceTimeoutError:
            if attempt >= max_retries - 1:
                raise
        except HfHubHTTPError as e:
            code = e.response.status_code
            if code not in _RETRYABLE_HF_HTTP_STATUSES or attempt >= max_retries - 1:
                raise
        delay = base_delay_s * (attempt + 1)
        time.sleep(delay)
    raise RuntimeError("zero-shot HF inference retries exhausted")


def add_zero_shot_classification_hf_inference(
    df: pd.DataFrame,
    candidate_labels: List[str],
    *,
    text_column: str = "text",
    hypothesis_template: str = "This text is about {}",
    multi_label: bool = True,
    model: str = DEFAULT_ZEROSHOT_MODEL,
    device: int = -1,
    text_max_chars: Optional[int] = ZEROSHOT_TEXT_MAX_CHARS_DEFAULT,
    passing_score_threshold: Optional[float] = None,
) -> pd.DataFrame:
    """
    Same column contract as :func:`add_zero_shot_classification_local`.

    ``device`` is ignored (remote inference). ``HUGGINGFACE_API_KEY`` (or Prefect
    secret) is recommended for higher rate limits and gated models; optional for
    some public models.
    """
    del device  # hosted path has no local torch device
    if not candidate_labels:
        raise ValueError("candidate_labels must be non-empty")

    if text_column not in df.columns:
        raise ValueError(f"DataFrame missing text column {text_column!r}")

    token = _hf_token_optional()
    bill_to = get_hf_bill_to()
    if bill_to:
        client = InferenceClient(
            token=token,
            bill_to=bill_to,
            timeout=ZEROSHOT_HF_INFERENCE_TIMEOUT_S,
        )
    else:
        client = InferenceClient(
            token=token,
            timeout=ZEROSHOT_HF_INFERENCE_TIMEOUT_S,
        )

    labels_col: list[str] = []
    scores_col: list[str] = []
    top_label_col: list[str] = []
    top_score_col: list[Optional[float]] = []
    error_col: list[str] = []

    for _, row in df.iterrows():
        raw = row.get(text_column)
        if raw is None or pd.isna(raw):
            text = ""
        else:
            text = _truncate(str(raw), text_max_chars)
        err_msg = ""
        try:
            labels, scores = _call_with_model_loading_retry(
                client,
                model,
                text,
                candidate_labels,
                hypothesis_template,
                multi_label,
            )
        except Exception as e:
            err_msg = f"{type(e).__name__}: {e}"[:2000]
            sid = row.get("story_id", "")
            if sid is None or (isinstance(sid, float) and pd.isna(sid)):
                sid = ""
            logger.warning(
                "zeroshot hf_inference failed story_id=%s: %s",
                sid,
                err_msg,
            )
            labels, scores = [], []
            top_label_col.append(ZEROSHOT_UNKNOWN_LABEL)
            top_score_col.append(None)
            labels_col.append(json.dumps(labels))
            scores_col.append(json.dumps(scores))
            error_col.append(err_msg)
            continue

        labels_col.append(json.dumps(labels))
        scores_col.append(json.dumps(scores))
        top_label_col.append(labels[0] if labels else "")
        top_score_col.append(scores[0] if scores else None)
        error_col.append(err_msg)

    out = df.copy()
    out["zeroshot_labels_json"] = labels_col
    out["zeroshot_scores_json"] = scores_col
    out["zeroshot_top_label"] = top_label_col
    out["zeroshot_top_score"] = top_score_col
    out["zeroshot_error"] = error_col

    if passing_score_threshold is not None:
        out = _append_passing_threshold_column(
            out, candidate_labels, passing_score_threshold
        )

    return out
