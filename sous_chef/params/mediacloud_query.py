"""
Base model for MediaCloud query parameters.

This model provides the standard parameters needed for querying MediaCloud
for news articles. It can be inherited by flow parameter models to avoid
duplication.
"""
from datetime import date
from typing import ClassVar, List

from pydantic import BaseModel, Field


class MediacloudQuery(BaseModel):
    """Base model for MediaCloud query parameters."""
    
    # Component hint for frontend grouping 
    _component_hint: ClassVar[str] = "MediacloudQuery"
    
    query: str
    collection_ids: List[int] = []
    source_ids: List[int] = []
    start_date: date
    end_date: date
    dedup_articles: bool = Field(
        default=False,
        title="Deduplicate articles",
        description=(
            "If enabled, remove duplicate stories by normalized title, keeping the "
            "earliest publish date only. Deduplication happens in the MediaCloud "
            "discovery step before downstream processing."
        ),
    )
