"""
MediaCloud-related artifacts.
"""
from typing import List, Optional, ClassVar
from datetime import date
from .base import BaseArtifact


class MediacloudQuerySummary(BaseArtifact):
    """
    Artifact summarizing a MediaCloud query execution.
    
    Contains the full query context and summary statistics from the results.
    This artifact provides a structured way to display query information
    in the frontend, including the query parameters and result counts.
    
    Example:
        summary = MediacloudQuerySummary(
            query="climate change",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            collection_ids=[1, 2],
            source_ids=[],
            story_count=150
        )
    """
    artifact_type: ClassVar[str] = "mediacloud_query_summary"
    
    # Query context
    query: str
    start_date: date
    end_date: date
    collection_ids: List[int] = []
    source_ids: List[int] = []
    
    # Summary statistics
    story_count: int
    total_stories: Optional[int] = None  # If pagination was used
    
    def _summary(self) -> str:
        """Generate a human-readable summary."""
        collections_str = (
            f"{len(self.collection_ids)} collections" 
            if self.collection_ids 
            else "all collections"
        )
        sources_str = (
            f"{len(self.source_ids)} sources" 
            if self.source_ids 
            else "all sources"
        )
        return (
            f"Query: '{self.query}' | "
            f"Date range: {self.start_date} to {self.end_date} | "
            f"Scope: {collections_str}, {sources_str} | "
            f"Stories: {self.story_count}"
        )
