"""
MediaCloud-related artifacts.
"""
from typing import ClassVar, List, Optional
from datetime import date

from .base import BaseArtifact
from .file_upload import FileUploadArtifact


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

    # Optional deduplication statistics for this query
    dedup_summary: Optional["ArticleDeduplicationSummary"] = None
    
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
    
    def get_artifact_description(self) -> str:
        """Generate a description for Prefect artifact display."""
        return f"MediaCloud Query Summary: '{self.query}' ({self.story_count} stories)"


class ArticleDeduplicationSummary(BaseArtifact):
    """
    Artifact summarizing article-level deduplication.

    Captures high-level statistics about how many stories were removed as
    duplicates, as well as basic configuration context. Detailed duplicates
    can be exported separately as a CSV if needed.
    """

    artifact_type: ClassVar[str] = "article_deduplication_summary"

    # Input/output counts
    input_story_count: int
    deduplicated_story_count: int
    duplicate_story_count: int

    # Configuration context
    dedup_by_title: bool = True
    dedup_by_text: bool = False
    dedup_title_column: str = "title"
    dedup_text_column: str = "content"
    dedup_date_column: str = "publish_date"

    # Optional CSV export of detailed duplicates
    duplicates_file: Optional[FileUploadArtifact] = None

    def _summary(self) -> str:
        return (
            f"Deduplicated {self.input_story_count} stories -> "
            f"{self.deduplicated_story_count} unique "
            f"({self.duplicate_story_count} duplicates removed)"
        )

    def get_artifact_description(self) -> str:
        return (
            f"Article Deduplication: {self.input_story_count} -> "
            f"{self.deduplicated_story_count} stories "
            f"({self.duplicate_story_count} duplicates removed)"
        )