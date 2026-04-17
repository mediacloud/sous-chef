"""
Parameters for zero-shot classification flows (Kitchen / frontend grouping).
"""
from typing import ClassVar, List, Optional

from pydantic import BaseModel, Field, field_validator


class ZeroShotClassificationParams(BaseModel):
    """Shared zero-shot classification fields; use with flow-specific param models."""

    _component_hint: ClassVar[str] = "ZeroShotClassificationParams"

    classification_labels: List[str] = Field(
        default_factory=lambda: [
            "politics",
            "economy",
            "environment",
            "technology",
            "health",
        ],
        title="Classification labels",
        description='Verbalized class names passed to the zero-shot model (e.g. "politics", "sports").',
    )
    hypothesis_template: str = Field(
        default="This text is about {}",
        title="Hypothesis template",
        description='NLI hypothesis with "{}" where the label is inserted (Hugging Face zero-shot format).',
    )
    multi_label: bool = Field(
        default=True,
        title="Multi-label",
        description="If true, scores all labels; if false, the pipeline picks a single best label.",
    )
    max_stories: Optional[int] = Field(
        default=None,
        ge=1,
        title="Max stories to classify",
        description="Optional cap after querying MediaCloud (recommended for local CPU demos).",
    )
    zeroshot_score_threshold: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        title="Score threshold (optional)",
        description=(
            "If set: summary distribution counts a label per story when its score ≥ this value; "
            "adds zeroshot_labels_passing_threshold_json to the export. "
            "If unset: distribution uses only the single top label per story. "
            "Ignored when Top-N labels is set."
        ),
    )
    zeroshot_top_n: Optional[int] = Field(
        default=None,
        ge=1,
        title="Top-N labels (optional)",
        description=(
            "If set and non-zero: select the highest-scoring N labels per story and "
            "ignore score threshold for selection and summary distribution."
        ),
    )

    @field_validator("hypothesis_template")
    @classmethod
    def _must_contain_brace_placeholder(cls, v: str) -> str:
        if "{}" not in v:
            raise ValueError('hypothesis_template must contain "{}" for the class label')
        return v

    @field_validator("classification_labels")
    @classmethod
    def _non_empty_labels(cls, v: List[str]) -> List[str]:
        stripped = [s.strip() for s in v if s and str(s).strip()]
        if not stripped:
            raise ValueError("classification_labels must contain at least one non-empty label")
        return stripped
