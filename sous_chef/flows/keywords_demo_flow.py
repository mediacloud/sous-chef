"""
Simple demo flow that extracts keywords from news articles.

This flow demonstrates:
- Querying MediaCloud for articles
- Extracting keywords from each article
- Keeping keywords associated with their source text in a DataFrame

Can run with or without Prefect.
"""
from typing import Dict, Any

from ..flow import register_flow
from ..params.mediacloud_query import MediacloudQuery
from ..params.csv_export import CsvExportParams
from ..params.email_recipient import EmailRecipientParam
from ..tasks.discovery_tasks import query_online_news
from ..tasks.keyword_tasks import extract_keywords
from ..tasks.aggregator_tasks import top_n_unique_values
from ..tasks.export_tasks import csv_to_b2
from ..tasks.email_tasks import send_email, send_templated_email, send_run_summary_email
from ..utils import create_url_safe_slug, get_logger

class KeywordsDemoParams(MediacloudQuery, CsvExportParams, EmailRecipientParam):
    """Parameters for the keywords demo flow."""
    top_n: int = 50  # Number of keywords to extract per article


@register_flow(
    name="yake_keywords",
    description="Demo: Extract keywords from news articles matching a query using YAKE",
    params_model=KeywordsDemoParams,
    log_prints=True
)
def keywords_demo_flow(params: KeywordsDemoParams) -> Dict[str, Any]:
    """
    Extract keywords from news articles matching a query.
    
    This flow:
    1. Queries MediaCloud for articles matching the query
    2. Extracts keywords from each article's text
    3. Aggregates keywords to find the top 50 most common keywords
    4. Exports results to B2 and returns artifacts
    
    Args:
        params: Flow parameters
        
    Returns:
        Dictionary containing only artifact objects:
        - query_summary: MediacloudQuerySummary artifact with query context and statistics
        - b2_artifact: FileUploadArtifact with upload details for the exported CSV
    """
    logger = get_logger()
    logger.info("starting keyword demo run")
    # Step 1: Query MediaCloud for articles
    articles, query_summary = query_online_news(
        query=params.query,
        collection_ids=params.collection_ids,
        source_ids=params.source_ids,
        start_date=params.start_date,
        end_date=params.end_date
    )

    
    # Step 2: Extract keywords from each article
    # This adds a 'keywords' column to the DataFrame
    articles = extract_keywords(
        articles,
        text_column="text",
        language_column="language",
        top_n=params.top_n
    )
    
    # Step 3: Aggregate keywords to find the top 50 most common keywords
    top_keywords = top_n_unique_values(
        articles,
        column="keywords",
        top_n=50
    )

    # Optional Step 4: Export top keywords to Backblaze B2 as CSV
 
    slug = create_url_safe_slug(params.query)
    object_name = (
            f"{params.b2_object_prefix}/DATE/{slug}-top-keywords.csv"
    )
    print(f"calling csv_to_b2 with object_name: {object_name}")
    b2_metadata, b2_artifact = csv_to_b2(
        top_keywords,
        object_name=object_name,
        add_date_slug=params.b2_add_date_slug,
        ensure_unique=params.b2_ensure_unique,
    )
    
    # Send email notification if recipients are specified
    if params.email_to:
        email_result = send_run_summary_email(
            email_to=params.email_to,
            query_summary=query_summary,
            b2_artifact=b2_artifact,
            flow_name="keywords_demo",
            query=params.query
        )


    # Return only artifact objects - these are saved as Prefect artifacts
    return {
        "query_summary": query_summary,
        "b2_artifact": b2_artifact,
    }
