"""
Base artifact class for structuring flow outputs.

Artifacts provide structured, serializable output types that can be:
- Automatically converted to Prefect table artifacts
- Styled by the frontend based on artifact type
- Displayed with useful string representations
"""
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, ClassVar
from abc import ABC


class BaseArtifact(BaseModel, ABC):
    """
    Base class for all artifacts.
    
    Artifacts structure output data similar to how parameter schemas
    structure input data. They provide:
    - Type identification for frontend styling
    - Serialization to dict/table format for Prefect
    - Useful string representations
    
    All artifacts should inherit from this class and set a unique
    `artifact_type` class variable.
    
    Example:
        class MyArtifact(BaseArtifact):
            artifact_type: ClassVar[str] = "my_artifact"
            
            field1: str
            field2: int
            
            def _summary(self) -> str:
                return f"My artifact: {self.field1}"
    """
    
    # Artifact type identifier for frontend styling
    artifact_type: ClassVar[str] = "base"
    
    def to_table_row(self) -> Dict[str, Any]:
        """
        Convert artifact to a single-row dict suitable for Prefect table artifacts.
        
        Returns a flat dict with all fields serialized to JSON-compatible types.
        The `_artifact_type` field is added automatically for frontend identification.
        
        Returns:
            Dict with all artifact fields plus `_artifact_type` field
        """
        data = self.model_dump()
        # Add artifact type for frontend identification
        data["_artifact_type"] = self.artifact_type
        return data
    
    def to_table(self) -> list[Dict[str, Any]]:
        """
        Convert artifact to table format (list of rows).
        
        Most artifacts are single-row, but this allows for multi-row artifacts
        if needed in the future.
        
        Returns:
            List containing a single dict (the table row)
        """
        return [self.to_table_row()]
    
    def __str__(self) -> str:
        """Canonical string representation of the artifact."""
        return f"{self.__class__.__name__}({self._summary()})"
    
    def _summary(self) -> str:
        """
        Generate a summary string for display.
        
        Override this method in subclasses to provide
        human-readable summaries.
        
        Returns:
            String summary of the artifact
        """
        return self.model_dump_json()
    
    class Config:
        """Pydantic config for artifacts."""
        # Allow extra fields for extensibility
        extra = "allow"
        # Use enum values in JSON
        use_enum_values = True
