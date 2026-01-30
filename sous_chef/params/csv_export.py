"""
Base model for CSV export parameters.

This model provides the standard parameters needed for exporting data to
Backblaze B2 as CSV files. It can be inherited by flow parameter models
to avoid duplication.
"""
from pydantic import BaseModel
from typing import Optional


class CsvExportParams(BaseModel):
    """Base model for CSV export parameters."""
    
    # Component hint for frontend grouping (Phase 2)
    _component_hint: str = "CsvExportParams"
    
    b2_bucket: Optional[str] = None
    b2_object_prefix: str = "sous-chef-output"
    b2_add_date_slug: bool = True
    b2_ensure_unique: bool = True
