from typing import Optional, List
import re
from pydantic import BaseModel

from ..flow import register_flow, BaseFlowOutput
from ..runtime import mark_step
from ..params.mediacloud_query import MediacloudQuery
from ..params.csv_export import CsvExportParams
from ..params.email_recipient import EmailRecipientParam
from ..params.webhook_callback import WebhookCallbackParam
from ..artifacts import MediacloudQuerySummary, FileUploadArtifact
from ..tasks.discovery_tasks import query_online_news
from ..tasks.deduplication_tasks import deduplicate_on_title_source
from ..tasks.tokenization_tasks import matching_sentences
from ..tasks.sentiment_tasks import targeted_sentiment
from ..tasks.export_tasks import csv_to_b2
from ..tasks.email_tasks import send_run_summary_email
from ..utils import create_url_safe_slug


class TargetedSentimentParams(MediacloudQuery, CsvExportParams, EmailRecipientParam, WebhookCallbackParam):
    """Parameters for flow."""
    spacy_model: str = "en_core_web_sm"  # SpaCy model to use for NER
    inclusion_filters: Optional[List[re.Pattern]] = None
    target_entity: str


class TargetedSentimentFlowOutput(BaseFlowOutput):
    """Output artifacts for the targeted sentiment flow."""
    query_summary: MediacloudQuerySummary
    b2_artifact: FileUploadArtifact


@register_flow(
    name="targeted_sentiment",
    description="Split docs into sentences, filter to sentences matching inclusion criteria pattern(s), add sentiment score towards target entity",
    params_model=TargetedSentimentParams,
    output_model=TargetedSentimentFlowOutput,
    log_prints=True
)
def targeted_sentiment_flow(params: TargetedSentimentParams) -> TargetedSentimentFlowOutput:
    # Step 1: Query MediaCloud for articles
    articles, query_summary = query_online_news(
        query=params.query,
        collection_ids=params.collection_ids,
        source_ids=params.source_ids,
        start_date=params.start_date,
        end_date=params.end_date,
        dedup_strategy=params.dedup_strategy,
        upload_dedup_summary=params.upload_dedup_summary,
    )
    
    # Step 2: Deduplicate stories
    mark_step("title_source_dedup_start", meta={"articles": len(articles)})
    deduplicaed_articles = deduplicate_on_title_source(
        articles,
    )
    mark_step("title_source_dedup_end", meta={"articles": len(deduplicaed_articles)})
    
    # Step 3: Get matching sentences
    mark_step("sentence_matching_start")
    sentences_df = matching_sentences(
        deduplicaed_articles,
        model=params.spacy_model,
        inclusion_filters=params.inclusion_filters,
    )
    mark_step("sentence_matching_end", meta={"rows": len(sentences_df)})

    # Step 4: Add targeted sentiment scores
    mark_step("targeted_sentiment_start", meta={"rows": len(sentences_df)})
    sentences_with_sentiment_df = targeted_sentiment(
        sentences_df,
        sentiment_target=params.target_entity,
    )
    mark_step("targeted_sentiment_end", meta={"rows": len(sentences_with_sentiment_df)})

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
    
    # Return FlowOutput model instance - these are saved as Prefect artifacts
    return TargetedSentimentFlowOutput(
        query_summary=query_summary,
        b2_artifact=b2_artifact,
    )
