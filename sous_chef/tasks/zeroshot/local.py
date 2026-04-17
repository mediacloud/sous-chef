"""Local zero-shot classification via ``transformers.pipeline``."""
from __future__ import annotations

import json
import logging
from typing import Any, List, Optional

import pandas as pd
import torch
from transformers import pipeline

from .common import _append_passing_threshold_column, _append_selected_labels_column, _truncate
from .config import (
    DEFAULT_ZEROSHOT_MODEL,
    ZEROSHOT_TEXT_MAX_CHARS_DEFAULT,
    ZEROSHOT_UNKNOWN_LABEL,
)

logger = logging.getLogger(__name__)


def _classify_one_pipeline(
    clf: Any,
    text: str,
    candidate_labels: List[str],
    hypothesis_template: str,
    multi_label: bool,
) -> tuple[list[str], list[float]]:
    if not text.strip():
        return [], []
    out = clf(
        text,
        candidate_labels,
        hypothesis_template=hypothesis_template,
        multi_label=multi_label,
    )
    labels = list(out["labels"])
    scores = [float(s) for s in out["scores"]]
    return labels, scores


def add_zero_shot_classification_local(
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
    top_n: Optional[int] = None,
) -> pd.DataFrame:
    if top_n is not None and int(top_n) < 1:
        raise ValueError("top_n must be >= 1 when provided")

    if not candidate_labels:
        raise ValueError("candidate_labels must be non-empty")

    if text_column not in df.columns:
        raise ValueError(f"DataFrame missing text column {text_column!r}")

    if device == -1:
        torch_device = -1
    else:
        torch_device = device if torch.cuda.is_available() else -1

    clf = pipeline(
        "zero-shot-classification",
        model=model,
        device=torch_device,
        framework="pt",
    )

    def classify_one(
        text: str,
        cands: List[str],
        hyp: str,
        multi: bool,
    ) -> tuple[list[str], list[float]]:
        return _classify_one_pipeline(clf, text, cands, hyp, multi)

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
            labels, scores = classify_one(
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
                "zeroshot local pipeline failed story_id=%s: %s",
                sid,
                err_msg,
            )
            labels, scores = [], []
            labels_col.append(json.dumps(labels))
            scores_col.append(json.dumps(scores))
            top_label_col.append(ZEROSHOT_UNKNOWN_LABEL)
            top_score_col.append(None)
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
    out, _ = _append_selected_labels_column(
        out,
        candidate_labels,
        passing_score_threshold=passing_score_threshold,
        top_n=top_n,
    )

    return out
