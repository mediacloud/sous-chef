"""
Artifacts for zero-shot classification flows.
"""
from __future__ import annotations

from typing import ClassVar, List, Optional

from .base import BaseArtifact


class ZeroShotClassificationSummary(BaseArtifact):
    """Summary of zero-shot labels used and how often each label appears in the run."""

    artifact_type: ClassVar[str] = "zeroshot_classification_summary"

    input_labels: List[str] = []
    """Class names provided to the model (stable order for counts)."""

    label_counts: List[int] = []
    """Counts aligned with input_labels (see distribution_mode)."""

    stories_classified: int = 0
    """Rows in the dataframe after classification (same as stories sent to export)."""

    stories_without_prediction: int = 0
    """Stories with no non-empty classification (empty text or no scores)."""

    summary_score_threshold: Optional[float] = None
    """
    If set, distribution counts each story per label whose score is >= this value.
    If null, distribution counts only the single top label per story.
    """

    distribution_mode: str = "top_label"
    """Either 'top_label' or 'threshold_ge' (mirrors how label_counts were computed)."""

    multi_label: bool = True
    hypothesis_template: str = "This text is about {}"
    model_id: str = ""

    def _summary(self) -> str:
        return (
            f"{self.stories_classified} stories, "
            f"{self.stories_without_prediction} without prediction "
            f"({self.distribution_mode})"
        )

    def get_artifact_description(self) -> str:
        mode = (
            f"threshold≥{self.summary_score_threshold}"
            if self.summary_score_threshold is not None
            else "top label per story"
        )
        return (
            f"Zero-shot summary: {self.stories_classified} stories "
            f"({mode}) across {len(self.input_labels)} labels"
        )
