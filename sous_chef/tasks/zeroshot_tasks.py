"""
Zero-shot classification tasks: Prefect entrypoints re-exporting the ``zeroshot`` package.
"""
from __future__ import annotations

from typing import List, Optional

import pandas as pd
from prefect import task

from .zeroshot import (
    DEFAULT_ZEROSHOT_MODEL,
    ZEROSHOT_CLASSIFY_DEVICE,
    ZEROSHOT_STORY_TEXT_COLUMN,
    ZEROSHOT_TEXT_MAX_CHARS_DEFAULT,
    _truncate,
    add_zero_shot_classification,
    compute_zero_shot_label_counts,
    get_zeroshot_backend,
    story_dataframe_for_zeroshot_csv,
)
from .zeroshot.config import (
    ZEROSHOT_BACKEND_ENV,
    ZEROSHOT_DEFAULT_STORY_COLUMNS,
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
    "zero_shot_classify_stories",
]


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
    backend: Optional[str] = None,
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
        backend=backend,
    )
