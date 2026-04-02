"""
Aboutness filter flow artifacts.
"""
from typing import ClassVar, List, Optional

from .base import BaseArtifact
from .file_upload import FileUploadArtifact
from .llm_cost import LLMCostSummary


class AboutnessFilterSummary(BaseArtifact):
    """
    Artifact summarizing how many articles were kept vs discarded by the
    aboutness filter.

    Provides counts and a simple score histogram for display in the run
    summary and frontend.
    """

    artifact_type: ClassVar[str] = "aboutness_filter_summary"

    total_scored: int = 0
    """Number of articles sent to the LLM for aboutness scoring."""

    passed_count: int = 0
    """Number of articles that passed the aboutness threshold."""

    discarded_count: int = 0
    """Number of articles discarded by the filter (did not pass threshold)."""

    threshold: float = 0.0
    """Aboutness score threshold used for filtering."""

    target_kind: Optional[str] = None
    """
    Optional high-level type of the aboutness target (e.g., 'geography',
    'person', 'organization', 'topic', 'other'). Mirrored from the
    AboutnessTargetKind enum when available.
    """

    about_target: Optional[str] = None
    """
    The specific target string being filtered for (the user's `about_target`).
    """

    score_histogram_counts: List[int] = []
    """
    Counts of articles in each aboutness score bin. Intended for a small,
    fixed number of bins (e.g., 10 bins across [0.0, 1.0]).
    """

    score_histogram_edges: List[float] = []
    """
    Bin edges for the score histogram, including both endpoints. For 10 equal
    bins across [0.0, 1.0], this would be [0.0, 0.1, ..., 1.0].
    """

    def _summary(self) -> str:
        return (
            f"{self.passed_count} passed, {self.discarded_count} discarded "
            f"(of {self.total_scored} scored, threshold={self.threshold})"
        )

    def get_artifact_description(self) -> str:
        return (
            f"Aboutness filter: {self.passed_count} articles passed, "
            f"{self.discarded_count} discarded from {self.total_scored} scored"
        )


class AboutnessScoringRunArtifact(BaseArtifact):
    """
    Task-level result for `score_aboutness_llm`: LLM usage/cost plus an optional
    B2 upload of all scored rows (before threshold filtering), mirroring how
    `query_online_news` can attach an optional dedup CSV via the query summary.
    """

    artifact_type: ClassVar[str] = "aboutness_scoring_run"

    llm_cost: LLMCostSummary
    scored_rows_csv: Optional[FileUploadArtifact] = None

    def _summary(self) -> str:
        base = str(self.llm_cost)
        if self.scored_rows_csv and getattr(self.scored_rows_csv, "object_key", None):
            return f"{base} | scored CSV uploaded"
        return base

    def get_artifact_description(self) -> str:
        return self.llm_cost.get_artifact_description()
