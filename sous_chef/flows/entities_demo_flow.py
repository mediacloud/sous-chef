"""
Demo flow that extracts named entities from news articles using SpaCy NER.

This flow demonstrates:
- Querying MediaCloud for articles
- Extracting named entities from each article using SpaCy
- Aggregating entities to find the top entities by type
- Filtering and sorting options for entity analysis

Can run with or without Prefect.
"""
from typing import Dict, Any, Optional

from ..flow import register_flow
from ..params.mediacloud_query import MediacloudQuery
from ..params.csv_export import CsvExportParams
from ..tasks.discovery_tasks import query_online_news
from ..tasks.extraction_tasks import extract_entities, top_n_entities
from ..tasks.export_tasks import csv_to_b2
from ..utils import create_url_safe_slug


class EntitiesDemoParams(MediacloudQuery, CsvExportParams):
    """Parameters for the entities demo flow."""
    spacy_model: str = "en_core_web_sm"  # SpaCy model to use for NER
    top_n: int = 20  # Number of top entities to return
    filter_type: Optional[str] = None  # Optional entity type filter (e.g., "PERSON", "ORG", "GPE")
    sort_by: str = "total"  # Sort by "total" or "percentage"


@register_flow(
    name="entities_demo",
    description="Demo: Extract named entities from news articles using SpaCy NER",
    params_model=EntitiesDemoParams,
    log_prints=True
)
def entities_demo_flow(params: EntitiesDemoParams) -> Dict[str, Any]:
    """
    Extract named entities from news articles matching a query.
    
    This flow:
    1. Queries MediaCloud for articles matching the query
    2. Extracts named entities from each article's text using SpaCy NER
    3. Aggregates entities to find the top entities (optionally filtered by type)
    4. Exports results to B2 and returns artifacts
    
    Args:
        params: Flow parameters
        
    Returns:
        Dictionary containing only artifact objects:
        - query_summary: MediacloudQuerySummary artifact with query context and statistics
        - b2_artifact: FileUploadArtifact with upload details for the exported CSV
    """
    
    # Step 1: Query MediaCloud for articles
    articles, query_summary = query_online_news(
        query=params.query,
        collection_ids=params.collection_ids,
        source_ids=params.source_ids,
        start_date=params.start_date,
        end_date=params.end_date
    )
    
    # Step 2: Extract named entities from each article
    # This adds an 'entities' column to the DataFrame
    # Each entity is a dict with {"text": "...", "type": "..."}
    articles = extract_entities(
        articles,
        text_column="text",
        model=params.spacy_model
    )
    
    # Step 3: Aggregate entities to find the top entities
    top_entities = top_n_entities(
        articles,
        entities_column="entities",
        top_n=params.top_n,
        filter_type=params.filter_type,
        sort_by=params.sort_by
    )
    
    # Optional Step 4: Export top entities to Backblaze B2 as CSV
    slug = create_url_safe_slug(params.query)
    filter_suffix = f"-{params.filter_type}" if params.filter_type else ""
    object_name = (
            f"{params.b2_object_prefix}/DATE/{slug}{filter_suffix}-top-entities.csv"
    )
    print(f"Exporting top entities to B2: {object_name}")
    b2_metadata, b2_artifact = csv_to_b2(
            top_entities,
            object_name=object_name,
            add_date_slug=params.b2_add_date_slug,
            ensure_unique=params.b2_ensure_unique,
    )
    
    # Return only artifact objects - these are saved as Prefect artifacts
    return {
        "query_summary": query_summary,
        "b2_artifact": b2_artifact,
    }
