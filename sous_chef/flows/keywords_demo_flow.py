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
from ..tasks.discovery_tasks import query_online_news
from ..tasks.keyword_tasks import extract_keywords
from ..tasks.aggregator_tasks import top_n_unique_values
from ..tasks.export_tasks import csv_to_b2
from ..tasks.email_tasks import send_email, send_templated_email, send_run_summary_email
from ..utils import create_url_safe_slug

class KeywordsDemoParams(MediacloudQuery, CsvExportParams):
    """Parameters for the keywords demo flow."""
    top_n: int = 50  # Number of keywords to extract per article


@register_flow(
    name="keywords_demo",
    description="Demo: Extract keywords from news articles matching a query",
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
    4. Returns articles with keywords and the top keywords summary
    
    The result keeps keywords associated with their source text,
    making it easy to see which keywords came from which article,
    and also provides a summary of the most common keywords across all articles.
    
    Args:
        params: Flow parameters
        
    Returns:
        Dictionary with:
        - article_count: Number of articles processed
        - top_keywords: DataFrame with top 50 most common keywords (value, count)
        - query: The search query used
        - b2_export: Optional dict with export metadata if B2 export was performed
    """

    # Step 1: Query MediaCloud for articles
    articles = query_online_news(
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
    b2_export = None
    if params.b2_bucket:
        slug = create_url_safe_slug(params.query)
        object_name = (
            f"{params.b2_object_prefix}/DATE/{slug}-top-keywords.csv"
        )
        print(f"calling csv_to_b2 with params: {params.b2_bucket}, {object_name}")
        b2_export = csv_to_b2(
            top_keywords,
            bucket_name=params.b2_bucket,
            object_name=object_name,
            add_date_slug=params.b2_add_date_slug,
            ensure_unique=params.b2_ensure_unique,
        )
        
    send_email(
        email_to="nano3.14@gmail.com",
        subject="test",
        msg="Test Sous-Chef output! Did we do it??"
        )


    ##Return values here are saved out later to prefect artifacts, and are transiently available via the buffet. 
    return {
        "article_count": len(articles),
        "query": params.query,
        "b2_export": b2_export,
    }
