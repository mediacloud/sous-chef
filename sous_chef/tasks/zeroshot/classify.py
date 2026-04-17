"""Dispatch zero-shot classification to local pipeline or Hugging Face Inference API."""
from __future__ import annotations

from typing import List, Optional

import pandas as pd

from .config import (
    DEFAULT_ZEROSHOT_MODEL,
    ZEROSHOT_TEXT_MAX_CHARS_DEFAULT,
    get_zeroshot_backend,
)
from .hf_inference import add_zero_shot_classification_hf_inference
from .local import add_zero_shot_classification_local


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
    top_n: Optional[int] = None,
    backend: Optional[str] = None,
) -> pd.DataFrame:
    """
    Add zero-shot classification columns to a story DataFrame.

    ``text_max_chars`` defaults here (like ``ngram_max`` on ``extract_keywords``); flows
    need not mirror it on ``params_model`` unless operators should tune it per run.

    Backend is ``local`` (``transformers.pipeline``) or ``hf_inference`` (hosted API),
    from the ``backend`` argument or ``ZEROSHOT_BACKEND`` environment variable.

    Adds:
      - zeroshot_labels_json: JSON list of labels (scores descending)
      - zeroshot_scores_json: JSON list of scores aligned with labels
      - zeroshot_top_label: highest-scoring label (or empty string)
      - zeroshot_top_score: score for top label (or None)
      - zeroshot_labels_passing_threshold_json: if passing_score_threshold is set,
        JSON list of input labels scoring >= threshold for that row.
      - zeroshot_labels_selected_json: selected labels for downstream consumers;
        top_n (if provided and >0) takes precedence over threshold.
    """
    kwargs = dict(
        text_column=text_column,
        hypothesis_template=hypothesis_template,
        multi_label=multi_label,
        model=model,
        device=device,
        text_max_chars=text_max_chars,
        passing_score_threshold=passing_score_threshold,
        top_n=top_n,
    )
    mode = get_zeroshot_backend(backend)
    if mode == "local":
        return add_zero_shot_classification_local(df, candidate_labels, **kwargs)
    return add_zero_shot_classification_hf_inference(df, candidate_labels, **kwargs)
