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


def _selected_zeroshot_labels_for_row(
    row: pd.Series,
    *,
    selection_mode: str,
) -> list[str]:
    """Return selected zero-shot labels for a row per selection mode."""
    if _zeroshot_row_error_message(row):
        return []

    if selection_mode in {"threshold_ge", "top_n"}:
        try:
            raw_selected = row.get("zeroshot_labels_selected_json")
            selected = json.loads(raw_selected if raw_selected is not None else "[]")
        except (json.JSONDecodeError, TypeError, ValueError):
            return []
        if not isinstance(selected, list):
            return []
        out: list[str] = []
        for lab in selected:
            if lab is None or (isinstance(lab, float) and pd.isna(lab)):
                continue
            key = str(lab).strip()
            if key:
                out.append(key)
        return out

    top = row.get("zeroshot_top_label")
    if top is None or (isinstance(top, float) and pd.isna(top)):
        return []
    top_str = str(top).strip()
    if not top_str or top_str == ZEROSHOT_UNKNOWN_LABEL:
        return []
    return [top_str]


def build_zero_shot_tag_scores_json_for_row(
    row: pd.Series,
    *,
    selection_mode: str = "top_label",
) -> str:
    """
    JSON object mapping each *exported* zero-shot tag to its score.

    Uses selected labels per ``selection_mode``:
      - ``threshold_ge`` / ``top_n`` read ``zeroshot_labels_selected_json``.
      - ``top_label`` uses ``zeroshot_top_label``.
    Scores always come from ``zeroshot_labels_json`` / ``zeroshot_scores_json``.
    Returns ``"{}"`` on inference error or when there is no selected tag.
    """
    if _zeroshot_row_error_message(row):
        return "{}"

    try:
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

    selected_labels = _selected_zeroshot_labels_for_row(
        row, selection_mode=selection_mode
    )
    if not selected_labels:
        return "{}"
    out: Dict[str, Optional[float]] = {}
    for lab in selected_labels:
        out[lab] = scores_by_label.get(lab)
    if selection_mode == "top_label" and len(out) == 1:
        key = next(iter(out))
        val = out[key]
        if val is None:
            return json.dumps({key: None}, ensure_ascii=False)
    try:
        return json.dumps(out, ensure_ascii=False)
    except (TypeError, ValueError):
        return "{}"


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


def _append_selected_labels_column(
    df: pd.DataFrame,
    candidate_labels: List[str],
    *,
    passing_score_threshold: Optional[float],
    top_n: Optional[int],
) -> tuple[pd.DataFrame, str]:
    """
    Append canonical selected labels column and return distribution mode.

    Precedence: ``top_n`` (if provided and > 0) overrides ``passing_score_threshold``.
    """
    selected_col: list[str] = []
    if top_n is not None and top_n > 0:
        mode = "top_n"
        n = int(top_n)
    elif passing_score_threshold is not None:
        mode = "threshold_ge"
        thr = float(passing_score_threshold)
    else:
        mode = "top_label"

    for _, row in df.iterrows():
        if _zeroshot_row_error_message(row):
            selected_col.append(json.dumps([]))
            continue
        try:
            labs = json.loads(row["zeroshot_labels_json"])
            scs = json.loads(row["zeroshot_scores_json"])
        except (json.JSONDecodeError, KeyError, TypeError):
            selected_col.append(json.dumps([]))
            continue

        if mode == "top_n":
            selected_col.append(json.dumps(list(labs[:n])))
            continue
        if mode == "threshold_ge":
            scores_by_label = dict(zip(labs, scs))
            passed = [
                lab
                for lab in candidate_labels
                if scores_by_label.get(lab) is not None
                and float(scores_by_label[lab]) >= thr
            ]
            selected_col.append(json.dumps(passed))
            continue
        if labs:
            selected_col.append(json.dumps([labs[0]]))
        else:
            selected_col.append(json.dumps([]))

    out = df.copy()
    out["zeroshot_labels_selected_json"] = selected_col
    return out, mode


def compute_zero_shot_label_counts(
    df: pd.DataFrame,
    input_labels: List[str],
    summary_score_threshold: Optional[float],
    summary_top_n: Optional[int] = None,
) -> Tuple[List[int], int, int]:
    """
    Compute per-label counts for the summary artifact.

    Selection precedence:
      1) ``summary_top_n`` (if provided and > 0): each story contributes counts
         for any of its top-N labels that are in ``input_labels``.
      2) ``summary_score_threshold``: each story contributes counts for labels
         meeting/exceeding the threshold.
      3) top-label mode: each story contributes at most one count.

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

    if summary_top_n is not None and summary_top_n > 0:
        n = int(summary_top_n)
        for _, row in df.iterrows():
            if _zeroshot_row_error_message(row):
                failed += 1
                continue
            try:
                labs = json.loads(row["zeroshot_labels_json"])
            except (json.JSONDecodeError, KeyError, TypeError):
                no_pred += 1
                continue
            if not labs:
                no_pred += 1
                continue
            any_hit = False
            for lab in labs[:n]:
                idx = label_to_idx.get(str(lab))
                if idx is None:
                    continue
                counts[idx] += 1
                any_hit = True
            if not any_hit:
                no_pred += 1
    elif summary_score_threshold is None:
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
