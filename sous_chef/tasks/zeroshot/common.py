"""Shared helpers for zero-shot classification (CSV export, summary counts, truncation)."""
from __future__ import annotations

import json
from typing import List, Optional, Tuple

import pandas as pd

from .config import ZEROSHOT_DEFAULT_STORY_COLUMNS


def _truncate(s: str, max_chars: Optional[int]) -> str:
    if max_chars is None or max_chars <= 0:
        return s
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    text = str(s)
    if len(text) <= max_chars:
        return text
    return text[:max_chars]


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
