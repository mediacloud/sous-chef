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

    # ------------------------------------------------------------------
    # Core serialization helpers
    # ------------------------------------------------------------------
    def to_table_row(self) -> Dict[str, Any]:
        """
        Convert artifact to a single-row dict suitable for Prefect table artifacts.

        Returns a flat dict with all fields serialized to JSON-compatible types.
        The `_artifact_type` field is added automatically for frontend
        identification.

        Dates and datetimes are converted to ISO format strings for JSON
        compatibility via Pydantic's `model_dump(mode="json")`.

        Returns:
            Dict with all artifact fields plus `_artifact_type` field.
        """
        # Use mode='json' to get JSON-serializable values (dates become strings)
        data = self.model_dump(mode="json")
        # Add artifact type for frontend identification
        data["_artifact_type"] = self.artifact_type
        return data

    def to_table(self) -> list[Dict[str, Any]]:
        """
        Convert artifact to table format (list of rows).

        Most artifacts are single-row, but this allows for multi-row artifacts
        if needed in the future.

        Returns:
            List containing a single dict (the table row).
        """
        return [self.to_table_row()]

    def get_artifact_description(self) -> str:
        """
        Human‑readable description for use in artifact UIs.

        Subclasses can override this to provide richer descriptions. The default
        is deliberately generic and based on the artifact type.
        """
        return f"{self.__class__.__name__} ({self.artifact_type})"

    def serialize_for_prefect(self) -> Dict[str, Any]:
        """
        Serialize this artifact into a structure suitable for Prefect
        `create_table_artifact`.

        This keeps all formatting decisions close to the artifact type, so the
        kitchen only needs to know how to call this method instead of
        hard‑coding serialization logic per type.

        Returns:
            Dict with at least:
                - ``table``: list of row dicts (JSON‑serializable)
                - ``description``: human‑readable description string
        """
        return {
            "table": self.to_table(),
            "description": self.get_artifact_description(),
        }
    
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
