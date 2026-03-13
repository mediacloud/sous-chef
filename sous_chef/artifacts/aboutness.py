"""
Aboutness filter flow artifacts.
"""
from typing import ClassVar, List

from .base import BaseArtifact


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
