"""
Prefect table artifact: one row per runtime event (recipe boundaries + mark_step).
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional

from pydantic import Field

from .base import BaseArtifact


class RuntimeTimelineArtifact(BaseArtifact):
    """Multi-row timeline for a single recipe run (see rows / to_table)."""

    artifact_type: ClassVar[str] = "runtime_timeline"

    recipe_name: str
    run_id: Optional[str] = None
    session_started_wall_iso: str
    session_finished_wall_iso: Optional[str] = None
    total_duration_ms: Optional[float] = None
    final_status: Optional[str] = None
    rows: List[Dict[str, Any]] = Field(default_factory=list)

    def to_table(self) -> List[Dict[str, Any]]:
        return [
            {**row, "_artifact_type": self.artifact_type} for row in self.rows
        ]

    def _summary(self) -> str:
        n = len(self.rows)
        dur = self.total_duration_ms
        if dur is not None:
            return f"{self.recipe_name}: {dur:.1f} ms, {n} events ({self.final_status})"
        return f"{self.recipe_name}: {n} events ({self.final_status})"

    def get_artifact_description(self) -> str:
        dur = self.total_duration_ms
        n = len(self.rows)
        if dur is not None:
            return f"Runtime timeline: {self.recipe_name} — {dur / 1000.0:.2f}s, {n} events"
        return f"Runtime timeline: {self.recipe_name} — {n} events"
