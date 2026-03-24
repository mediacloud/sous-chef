"""
Zero-shot text classification using Hugging Face NLI-style classifiers (e.g. BGE-M3 zeroshot).
"""
from __future__ import annotations

import json
from typing import Any, List, Optional, Tuple

import pandas as pd
import torch
from prefect import task
from transformers import pipeline


DEFAULT_ZEROSHOT_MODEL = "MoritzLaurer/bge-m3-zeroshot-v2.0"

# Flow-owned defaults (not user parameters): MediaCloud stories use `text`; CPU unless you change this constant.
ZEROSHOT_STORY_TEXT_COLUMN = "text"
ZEROSHOT_CLASSIFY_DEVICE = -1
# Truncation before inference (task default; not a Kitchen param unless a flow exposes it).
ZEROSHOT_TEXT_MAX_CHARS_DEFAULT = 12000

# Default metadata columns for CSV export (never includes full story `text`).
ZEROSHOT_DEFAULT_STORY_COLUMNS: List[str] = [
    "story_id",
    "title",
    "url",
    "publish_date",
    "media_name",
    "language",
]


def _truncate(s: str, max_chars: Optional[int]) -> str:
    if max_chars is None or max_chars <= 0:
        return s
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    text = str(s)
    if len(text) <= max_chars:
        return text
    return text[:max_chars]


def _classify_one(
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


def _append_passing_threshold_column(
    df: pd.DataFrame,
    candidate_labels: List[str],
    threshold: float,
) -> pd.DataFrame:
    thr = float(threshold)
    passing_col: list[str] = []
    for _, row in df.iterrows():
        try:
            labs = json.loads(row["zeroshot_labels_json"])
            scs = json.loads(row["zeroshot_scores_json"])
        except (json.JSONDecodeError, KeyError, TypeError):
            passing_col.append(json.dumps([]))
            continue
        scores_by_label = dict(zip(labs, scs))
        passed = [
            lab
            for lab in candidate_labels
            if scores_by_label.get(lab) is not None
            and float(scores_by_label[lab]) >= thr
        ]
        passing_col.append(json.dumps(passed))
    out = df.copy()
    out["zeroshot_labels_passing_threshold_json"] = passing_col
    return out


def compute_zero_shot_label_counts(
    df: pd.DataFrame,
    input_labels: List[str],
    summary_score_threshold: Optional[float],
) -> Tuple[List[int], int]:
    """
    Compute per-label counts for the summary artifact.

    If summary_score_threshold is None: each story contributes at most one count
    to the label matching zeroshot_top_label (if it is in input_labels).

    If summary_score_threshold is set: for each story, every input label whose
    score meets or exceeds the threshold increments that label's count.

    Returns:
        (label_counts aligned with input_labels, stories_without_prediction)
    """
    counts = [0] * len(input_labels)
    label_to_idx = {lab: i for i, lab in enumerate(input_labels)}
    no_pred = 0

    if summary_score_threshold is None:
        for _, row in df.iterrows():
            tl = row.get("zeroshot_top_label")
            if tl is None or (isinstance(tl, float) and pd.isna(tl)):
                no_pred += 1
                continue
            tl_str = str(tl).strip()
            if not tl_str:
                no_pred += 1
                continue
            idx = label_to_idx.get(tl_str)
            if idx is None:
                no_pred += 1
                continue
            counts[idx] += 1
    else:
        thr = float(summary_score_threshold)
        for _, row in df.iterrows():
            try:
                labs = json.loads(row["zeroshot_labels_json"])
                scs = json.loads(row["zeroshot_scores_json"])
            except (json.JSONDecodeError, KeyError, TypeError):
                no_pred += 1
                continue
            if not labs:
                no_pred += 1
                continue
            scores_by_label = dict(zip(labs, scs))
            any_hit = False
            for lab in input_labels:
                sc = scores_by_label.get(lab)
                if sc is not None and float(sc) >= thr:
                    counts[label_to_idx[lab]] += 1
                    any_hit = True
            if not any_hit:
                no_pred += 1

    return counts, no_pred


def story_dataframe_for_zeroshot_csv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a dataframe for B2 CSV export: drops full text and keeps metadata + zeroshot_*.

    ``text`` is never included. Uses ZEROSHOT_DEFAULT_STORY_COLUMNS (intersected with df)
    plus every column whose name starts with ``zeroshot_``.
    """
    zeroshot_cols = [c for c in df.columns if str(c).startswith("zeroshot_")]
    cols = [c for c in ZEROSHOT_DEFAULT_STORY_COLUMNS if c in df.columns]
    cols = cols + zeroshot_cols

    seen: set[str] = set()
    ordered: list[str] = []
    for c in cols:
        if c not in seen:
            seen.add(c)
            ordered.append(c)

    missing = [c for c in ordered if c not in df.columns]
    if missing:
        raise ValueError(f"Requested CSV columns not in dataframe: {missing}")

    return df.loc[:, ordered].copy()


def add_zero_shot_classification(
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
    Add zero-shot classification columns to a story DataFrame.

    ``text_max_chars`` defaults here (like ``ngram_max`` on ``extract_keywords``); flows
    need not mirror it on ``params_model`` unless operators should tune it per run.

    Adds:
      - zeroshot_labels_json: JSON list of labels (scores descending)
      - zeroshot_scores_json: JSON list of scores aligned with labels
      - zeroshot_top_label: highest-scoring label (or empty string)
      - zeroshot_top_score: score for top label (or None)
      - zeroshot_labels_passing_threshold_json: if passing_score_threshold is set,
        JSON list of input labels scoring >= threshold for that row.
    """
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

    labels_col: list[str] = []
    scores_col: list[str] = []
    top_label_col: list[str] = []
    top_score_col: list[Optional[float]] = []

    for _, row in df.iterrows():
        raw = row.get(text_column)
        if raw is None or pd.isna(raw):
            text = ""
        else:
            text = _truncate(str(raw), text_max_chars)
        labels, scores = _classify_one(
            clf,
            text,
            candidate_labels,
            hypothesis_template,
            multi_label,
        )
        labels_col.append(json.dumps(labels))
        scores_col.append(json.dumps(scores))
        top_label_col.append(labels[0] if labels else "")
        top_score_col.append(scores[0] if scores else None)

    out = df.copy()
    out["zeroshot_labels_json"] = labels_col
    out["zeroshot_scores_json"] = scores_col
    out["zeroshot_top_label"] = top_label_col
    out["zeroshot_top_score"] = top_score_col

    if passing_score_threshold is not None:
        out = _append_passing_threshold_column(
            out, candidate_labels, passing_score_threshold
        )

    return out


@task
def zero_shot_classify_stories(
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
    """Prefect task wrapper for add_zero_shot_classification."""
    return add_zero_shot_classification(
        df,
        candidate_labels,
        text_column=text_column,
        hypothesis_template=hypothesis_template,
        multi_label=multi_label,
        model=model,
        device=device,
        text_max_chars=text_max_chars,
        passing_score_threshold=passing_score_threshold,
    )
