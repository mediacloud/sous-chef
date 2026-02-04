"""
Task library for sous-chef flows.

Tasks are Prefect-decorated functions that can be used within flows.

Task Return Pattern
-------------------

Tasks that produce artifacts return `ArtifactResult[T]` tuples:
    - Use `ArtifactResult[ResultType]` as the return type annotation
    - Return tuple: (result, artifact)
    - The `ArtifactResult` type makes it immediately discoverable from the signature

Tasks without artifacts return their result directly (no tuple).

Example with artifact:
    from sous_chef.artifacts import ArtifactResult, MediacloudQuerySummary
    
    def query_online_news(...) -> ArtifactResult[pd.DataFrame]:
        df = ...
        summary = MediacloudQuerySummary(...)
        return df, summary
    
    # Usage in flows:
    articles, query_summary = query_online_news(...)

Example without artifact:
    def extract_entities(...) -> pd.DataFrame:
        df = ...
        return df
    
    # Usage in flows:
    articles = extract_entities(...)

This pattern ensures:
- Clear signal: `ArtifactResult` type = artifact present
- Less verbosity: Simple tasks don't require tuple unpacking
- Type safety: Explicit return types with IDE support
- Discoverability: Artifact presence visible in function signature
"""

from .discovery_tasks import query_online_news
from .keyword_tasks import extract_keywords
from .extraction_tasks import extract_entities, top_n_entities
from .aggregator_tasks import top_n_unique_values
from .export_tasks import csv_to_b2
from .email_tasks import send_email, send_templated_email, send_run_summary_email

__all__ = [
    "query_online_news",
    "extract_keywords",
    "extract_entities",
    "top_n_entities",
    "top_n_unique_values",
    "csv_to_b2",
    "send_email",
    "send_templated_email",
    "send_run_summary_email",
]
