"""
Artifact schemas for structuring flow outputs.

This module provides artifact classes that structure output data similar to how
parameter schemas structure input data. Artifacts are Pydantic models that:

1. Provide structured, validated output data
2. Serialize to Prefect table artifact format
3. Include type identifiers for frontend styling
4. Have useful string representations

Task Return Pattern
-------------------

Tasks that produce artifacts should use the `ArtifactResult` type alias:

    from sous_chef.artifacts import ArtifactResult, MediacloudQuerySummary
    
    def my_task(...) -> ArtifactResult[pd.DataFrame]:
        df = ...
        summary = MediacloudQuerySummary(...)
        return df, summary

Tasks without artifacts return their result directly (no tuple).

The `ArtifactResult[T]` type makes it immediately clear from the signature
that a task returns an artifact, and IDEs will show this in autocomplete.
"""

from typing import TypeVar, Tuple

from .base import BaseArtifact
from .mediacloud import MediacloudQuerySummary
from .file_upload import FileUploadArtifact

T = TypeVar('T')

ArtifactResult = Tuple[T, BaseArtifact]
"""
Type alias for tasks that return (result, artifact) tuples.

Tasks using this return type will return a tuple where:
- First element: The primary result (DataFrame, dict, etc.)
- Second element: A BaseArtifact instance for frontend display

This type alias makes it immediately discoverable from function signatures
which tasks return artifacts, without needing to inspect the implementation.

Example:
    def query_online_news(...) -> ArtifactResult[pd.DataFrame]:
        df = ...
        artifact = MediacloudQuerySummary(...)
        return df, artifact
    
    # Usage in flows:
    articles, summary = query_online_news(...)
"""

__all__ = [
    "BaseArtifact",
    "MediacloudQuerySummary",
    "FileUploadArtifact",
    "ArtifactResult",
]
