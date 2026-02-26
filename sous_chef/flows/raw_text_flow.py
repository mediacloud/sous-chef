"""
Flow that returns raw article text from MediaCloud queries.

This flow is admin-only and returns raw text that is restricted to staff users
with full-text access.
"""
from typing import Dict, Any, List

from ..flow import register_flow
from ..params.mediacloud_query import MediacloudQuery
from ..tasks.discovery_tasks import query_online_news
from ..utils import get_logger


class RawTextParams(MediacloudQuery):
    """Parameters for the raw-text recipe."""
    # Inherit query, dates, collections, etc. from MediacloudQuery
    pass


@register_flow(
    name="raw_text_demo",
    description="Return raw article texts for a MediaCloud query (admin-only)",
    params_model=RawTextParams,
    admin_only=True,
    restricted_fields={"raw_text": True},
    log_prints=True,
)
def raw_text_demo_flow(params: RawTextParams) -> Dict[str, Any]:
    """
    Fetch articles and return their raw text (admin-only, full-text-gated).
    
    This flow:
    1. Queries MediaCloud for articles matching the query
    2. Returns the raw text from each article
    3. The raw text is restricted and only visible to staff users with full-text access
    
    Args:
        params: Flow parameters including query, dates, collections, etc.
        
    Returns:
        Dictionary containing:
        - query_summary: MediacloudQuerySummary artifact with query context and statistics
        - raw_text: List of raw article texts (restricted to full-text authorized users)
    """
    logger = get_logger()
    logger.info("Starting raw_text_demo run")

    # Step 1: Query MediaCloud for articles
    articles, query_summary = query_online_news(
        query=params.query,
        collection_ids=params.collection_ids,
        source_ids=params.source_ids,
        start_date=params.start_date,
        end_date=params.end_date,
    )

    # Step 2: Extract raw text from articles
    # The articles DataFrame should have a 'text' column
    if "text" in articles.columns:
        texts: List[str] = articles["text"].tolist()
    else:
        logger.warning("No 'text' column found in articles DataFrame")
        texts = []

    logger.info(f"Extracted {len(texts)} article texts")

    # Return artifacts:
    # - query_summary is non-restricted (visible to all authorized users)
    # - raw_text is restricted (only visible to staff with full-text access)
    return {
        "query_summary": query_summary,
        "raw_text": texts,
    }
