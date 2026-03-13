"""
Base model for MediaCloud query parameters.

This model provides the standard parameters needed for querying MediaCloud
for news articles. It can be inherited by flow parameter models to avoid
duplication.
"""
from datetime import date
from enum import Enum
from typing import ClassVar, List

from pydantic import BaseModel, Field


class DedupStrategy(str, Enum):
    """Article-level deduplication strategy for MediaCloud stories."""

    none = "none"
    title_source = "title_source"
    title = "title"


class MediacloudQuery(BaseModel):
    """Base model for MediaCloud query parameters."""
    
    # Component hint for frontend grouping 
    _component_hint: ClassVar[str] = "MediacloudQuery"
    
    query: str
    collection_ids: List[int] = []
    source_ids: List[int] = []
    start_date: date
    end_date: date
    dedup_strategy: DedupStrategy = Field(
        default=DedupStrategy.none,
        title="Deduplication strategy",
        description=(
            "How to deduplicate MediaCloud stories. "
            "'none' keeps all stories; 'title_source' keeps one story per title+source; "
            "'title' keeps one story per title across all sources."
        ),
    )
    upload_dedup_summary: bool = Field(
        default=False,
        title="Upload dedup summary CSV",
        description=(
            "If enabled and a deduplication strategy is used, upload a CSV of dropped "
            "duplicate stories and link it from the deduplication summary."
        ),
    )
