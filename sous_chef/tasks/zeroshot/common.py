"""Shared helpers for zero-shot classification (CSV export, summary counts, truncation)."""
from __future__ import annotations

import json
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .config import ZEROSHOT_DEFAULT_STORY_COLUMNS, ZEROSHOT_UNKNOWN_LABEL


def _zeroshot_row_error_message(row: pd.Series) -> Optional[str]:
    """Non-empty error string if this row recorded an inference failure."""
    if "zeroshot_error" not in row.index:
        return None
    err = row.get("zeroshot_error")
    if err is None or (isinstance(err, float) and pd.isna(err)):
        return None
    s = str(err).strip()
    return s or None


def build_zero_shot_tag_scores_json_for_row(
    row: pd.Series,
    *,
    use_passing_threshold: bool,
) -> str:
    """
    JSON object mapping each *exported* zero-shot tag to its score.

    When ``use_passing_threshold`` is True, uses labels in
    ``zeroshot_labels_passing_threshold_json`` and scores from
    ``zeroshot_labels_json`` / ``zeroshot_scores_json``. Otherwise uses
    ``zeroshot_top_label`` and ``zeroshot_top_score`` (value ``null`` if score
    is missing). Returns ``"{}"`` on inference error or when there is no tag.
    """
    if _zeroshot_row_error_message(row):
        return "{}"

    if use_passing_threshold:
        try:
            raw_pass = row.get("zeroshot_labels_passing_threshold_json")
            passed = json.loads(raw_pass if raw_pass is not None else "[]")
            if not isinstance(passed, list):
                passed = []
            labs = json.loads(row["zeroshot_labels_json"])
            scs = json.loads(row["zeroshot_scores_json"])
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            return "{}"
        scores_by_label: Dict[str, float] = {}
        for lab, sc in zip(labs, scs):
            if lab is None or (isinstance(lab, float) and pd.isna(lab)):
                continue
            key = str(lab).strip()
            if not key:
                continue
            try:
                scores_by_label[key] = float(sc)
            except (TypeError, ValueError):
                continue
        out: Dict[str, float] = {}
        for lab in passed:
            if lab is None or (isinstance(lab, float) and pd.isna(lab)):
                continue
            key = str(lab).strip()
            if key in scores_by_label:
                out[key] = scores_by_label[key]
        return json.dumps(out, ensure_ascii=False)

    top = row.get("zeroshot_top_label")
    if top is None or (isinstance(top, float) and pd.isna(top)):
        return "{}"
    top_str = str(top).strip()
    if not top_str or top_str == ZEROSHOT_UNKNOWN_LABEL:
        return "{}"
    ts = row.get("zeroshot_top_score")
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return json.dumps({top_str: None}, ensure_ascii=False)
    try:
        score_f = float(ts)
    except (TypeError, ValueError):
        return json.dumps({top_str: None}, ensure_ascii=False)
    return json.dumps({top_str: score_f}, ensure_ascii=False)


def zeroshot_classification_failure_details(df: pd.DataFrame) -> List[Dict[str, str]]:
    """Structured entries for artifact / logs (story_id, title, error)."""
    if df.empty or "zeroshot_error" not in df.columns:
        return []
    out: list[dict[str, str]] = []
    for _, row in df.iterrows():
        msg = _zeroshot_row_error_message(row)
        if not msg:
            continue
        sid = row.get("story_id", "")
        if sid is None or (isinstance(sid, float) and pd.isna(sid)):
            sid = ""
        title = row.get("title", "")
        if title is None or (isinstance(title, float) and pd.isna(title)):
            title = ""
        out.append(
            {
                "story_id": str(sid),
                "title": str(title)[:500],
                "error": msg[:2000],
            }
        )
    return out


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
        if _zeroshot_row_error_message(row):
            passing_col.append(json.dumps([]))
            continue
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
) -> Tuple[List[int], int, int]:
    """
    Compute per-label counts for the summary artifact.

    If summary_score_threshold is None: each story contributes at most one count
    to the label matching zeroshot_top_label (if it is in input_labels).

    If summary_score_threshold is set: for each story, every input label whose
    score meets or exceeds the threshold increments that label's count.

    Rows with a non-empty ``zeroshot_error`` are inference failures: they do not
    contribute to label counts or ``stories_without_prediction``.

    Returns:
        (label_counts aligned with input_labels, stories_without_prediction,
         stories_classification_failed)
    """
    counts = [0] * len(input_labels)
    label_to_idx = {lab: i for i, lab in enumerate(input_labels)}
    no_pred = 0
    failed = 0

    if summary_score_threshold is None:
        for _, row in df.iterrows():
            if _zeroshot_row_error_message(row):
                failed += 1
                continue
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
            if _zeroshot_row_error_message(row):
                failed += 1
                continue
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

    return counts, no_pred, failed


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
