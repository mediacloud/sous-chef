from typing import Dict, Any, Optional, List
import re
from ..flow import register_flow
from ..params.mediacloud_query import MediacloudQuery
from ..params.csv_export import CsvExportParams
from ..params.email_recipient import EmailRecipientParam
from ..tasks.discovery_tasks import query_online_news
from ..tasks.deduplication_tasks import deduplicate_on_title_source
from ..tasks.tokenization_tasks import matching_sentences
from ..tasks.sentiment_tasks import targeted_sentiment
from ..tasks.export_tasks import csv_to_b2
from ..tasks.email_tasks import send_run_summary_email
from ..utils import create_url_safe_slug


class TargetedSentimentParams(MediacloudQuery, CsvExportParams, EmailRecipientParam):
    """Parameters for flow."""
    spacy_model: str = "en_core_web_sm"  # SpaCy model to use for NER
    inclusion_filters: Optional[List[re.Pattern]] = None
    target_entity: str


@register_flow(
    name="targeted_sentiment",
    description="Split docs into sentences, filter to sentences matching inclusion criteria pattern(s), add sentiment score towards target entity",
    params_model=TargetedSentimentParams,
    log_prints=True
)
def targeted_sentiment_flow(params: TargetedSentimentParams) -> Dict[str, Any]:
    # Step 1: Query MediaCloud for articles
    articles, query_summary = query_online_news(
        query=params.query,
        collection_ids=params.collection_ids,
        source_ids=params.source_ids,
        start_date=params.start_date,
        end_date=params.end_date
    )
    
    # Step 2: Deduplicate stories
    deduplicaed_articles = deduplicate_on_title_source(
        articles,
    )
    
    # Step 3: Get matching sentences
    sentences_df = matching_sentences(
        deduplicaed_articles,
        model=params.spacy_model,
        inclusion_filters=params.inclusion_filters,
    )

    # Step 4: Add targeted sentiment scores
    sentences_with_sentiment_df = targeted_sentiment(
        sentences_df,
        sentiment_target=params.target_entity,
    )

    # Optional Step 5: Export to Backblaze B2 as CSV
    slug = create_url_safe_slug(params.query)
    object_name = (
            f"{params.b2_object_prefix}/DATE/{slug}-targeted-sentiment.csv"
    )
    print(f"Exporting sentences to B2: {object_name}")
    b2_metadata, b2_artifact = csv_to_b2(
            sentences_with_sentiment_df,
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
            flow_name="targeted_sentiment",
            query=params.query
        )
    
    # Return only artifact objects - these are saved as Prefect artifacts
    return {
        "query_summary": query_summary,
        "b2_artifact": b2_artifact,
    }
