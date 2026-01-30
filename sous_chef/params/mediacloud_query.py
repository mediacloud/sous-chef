"""
Base model for MediaCloud query parameters.

This model provides the standard parameters needed for querying MediaCloud
for news articles. It can be inherited by flow parameter models to avoid
duplication.
"""
from pydantic import BaseModel
from typing import List, ClassVar
from datetime import date


class MediacloudQuery(BaseModel):
    """Base model for MediaCloud query parameters."""
    
    # Component hint for frontend grouping 
    _component_hint: ClassVar[str] = "MediacloudQuery"
    
    query: str
    collection_ids: List[int] = []
    source_ids: List[int] = []
    start_date: date
    end_date: date
