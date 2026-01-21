"""
Simple demo flow that extracts keywords from news articles.

This flow demonstrates:
- Querying MediaCloud for articles
- Extracting keywords from each article
- Keeping keywords associated with their source text in a DataFrame

Can run with or without Prefect.
"""
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import date
import pandas as pd

from ..flow import register_flow
from ..tasks.discovery_tasks import query_online_news
from ..tasks.keyword_tasks import extract_keywords
from ..tasks.aggregator_tasks import top_n_unique_values
from ..tasks.export_tasks import csv_to_b2


class KeywordsDemoParams(BaseModel):
    """Parameters for the keywords demo flow."""
    query: str
    collection_ids: List[int] = []
    source_ids: List[int] = []
    start_date: date
    end_date: date
    top_n: int = 50  # Number of keywords to extract per article
    # Optional Backblaze B2 export settings
    b2_bucket: Optional[str] = "sous-chef-output"
    b2_object_prefix: str = "sous-chef-two"
    b2_add_date_slug: bool = True
    b2_ensure_unique: bool = True


@register_flow(
    name="keywords_demo",
    description="Demo: Extract keywords from news articles matching a query",
    params_model=KeywordsDemoParams
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
        object_name = (
            f"{params.b2_object_prefix}/DATE/{params.query}-top-keywords.csv"
        )
        b2_export = csv_to_b2(
            top_keywords,
            bucket_name=params.b2_bucket,
            object_name=object_name,
            add_date_slug=params.b2_add_date_slug,
            ensure_unique=params.b2_ensure_unique,
        )
        

    return {
        "article_count": len(articles),
        "query": params.query,
        "b2_export": b2_export,
    }
