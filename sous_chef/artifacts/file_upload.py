"""
File upload artifacts.
"""
from typing import Optional, List, ClassVar, Dict, Any
from .base import BaseArtifact


class FileUploadArtifact(BaseArtifact):
    """
    Artifact representing a file upload to storage (e.g., B2).
    
    Contains the URL and metadata needed to understand and access the file.
    This artifact provides structured information about uploaded files
    for display in the frontend.
    
    Example:
        artifact = FileUploadArtifact(
            url="https://example.com/bucket/file.csv",
            bucket="my-bucket",
            object_key="path/to/file.csv",
            file_type="csv",
            columns_saved=["col1", "col2"],
            row_count=100
        )
    """
    artifact_type: ClassVar[str] = "file_upload"
    
    # File location
    url: Optional[str] = None
    bucket: str
    object_key: str
    
    # File metadata
    file_type: str = "csv"  # csv, json, etc.
    columns_saved: Optional[List[str]] = None  # For CSV files
    row_count: Optional[int] = None
    
    def _summary(self) -> str:
        """Generate a human-readable summary."""
        if self.url:
            return f"File uploaded: {self.url}"
        else:
            return f"File uploaded: {self.bucket}/{self.object_key}"
    
    def to_table_row(self) -> Dict[str, Any]:
        """
        Override to ensure URL is prominently displayed and include s3_url alias.
        
        The `s3_url` field is included for frontend compatibility with existing
        code that looks for this field name.
        """
        row = super().to_table_row()
        # Ensure URL is first for easy access
        if self.url:
            row["download_url"] = self.url
            row["s3_url"] = self.url  # Frontend compatibility
        return row
