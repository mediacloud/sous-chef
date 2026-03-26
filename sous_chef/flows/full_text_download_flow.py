"""
Flow that downloads full text from MediaCloud queries and exports to CSV.

This flow queries MediaCloud for articles, extracts the full text along with
basic metadata, and exports it as a CSV file to Backblaze B2.
"""
import pandas as pd
from ..flow import register_flow, BaseFlowOutput
from ..runtime import mark_step
from ..params.mediacloud_query import MediacloudQuery
from ..params.csv_export import CsvExportParams
from ..params.email_recipient import EmailRecipientParam
from ..params.webhook_callback import WebhookCallbackParam
from ..artifacts import (
    MediacloudQuerySummary,
    FileUploadArtifact,
)
from ..tasks.discovery_tasks import query_online_news
from ..tasks.export_tasks import csv_to_b2
from ..tasks.email_tasks import send_run_summary_email
from ..utils import create_url_safe_slug, get_logger


class FullTextDownloadParams(
    MediacloudQuery,
    CsvExportParams,
    EmailRecipientParam,
    WebhookCallbackParam,
):
    """Parameters for the full-text download flow."""


class FullTextDownloadFlowOutput(BaseFlowOutput):
    """Output artifacts for the full-text download flow."""

    query_summary: MediacloudQuerySummary
    b2_artifact: FileUploadArtifact


@register_flow(
    name="full_text_download",
    description="Download full article text from MediaCloud queries and export to CSV",
    params_model=FullTextDownloadParams,
    output_model=FullTextDownloadFlowOutput,
    admin_only=True,
    restricted_fields={"full_text_data": True},
    log_prints=True,
)
def full_text_download_flow(params: FullTextDownloadParams) -> FullTextDownloadFlowOutput:
    """
    Download full text from articles matching a MediaCloud query.
    
    This flow:
    1. Queries MediaCloud for articles matching the query
    2. Extracts full text along with basic metadata (title, URL, date, etc.)
    3. Exports the data to CSV and uploads to Backblaze B2
    4. Sends email notification if recipients are specified
    
    Args:
        params: Flow parameters including query, dates, collections, B2 settings, and email
        
    Returns:
        Dictionary containing artifact objects:
        - query_summary: MediacloudQuerySummary artifact with query context and statistics
        - b2_artifact: FileUploadArtifact with upload details for the exported CSV
        - full_text_data: DataFrame with full text (restricted to full-text authorized users)
    """
    logger = get_logger()
    logger.info("Starting full_text_download flow")

    # Step 1: Query MediaCloud for articles (with optional deduplication)
    articles, query_summary = query_online_news(
        query=params.query,
        collection_ids=params.collection_ids,
        source_ids=params.source_ids,
        start_date=params.start_date,
        end_date=params.end_date,
        dedup_strategy=params.dedup_strategy,
        upload_dedup_summary=params.upload_dedup_summary,
    )

    logger.info(f"Retrieved {len(articles)} articles")

    # Use the returned articles directly; any deduplication has already been applied
    articles_to_use = articles

    # Step 2: Extract full text with metadata
    # Select relevant columns for the CSV export
    # Common MediaCloud columns: stories_id, title, url, text, publish_date, media_id
    columns_to_include = []
    
    # Always include text if available
    if "text" in articles_to_use.columns:
        columns_to_include.append("text")
    
    # Include metadata columns if available
    metadata_columns = ["stories_id", "title", "url", "publish_date", "media_id", "language"]
    for col in metadata_columns:
        if col in articles_to_use.columns:
            columns_to_include.append(col)
    
    # Create DataFrame with selected columns
    mark_step("full_text_selection_start", meta={"articles": len(articles_to_use)})
    if columns_to_include:
        full_text_df = articles_to_use[columns_to_include].copy()
    else:
        # Fallback: if no expected columns, just use the text column or all columns
        if "text" in articles_to_use.columns:
            full_text_df = articles_to_use[["text"]].copy()
        else:
            logger.warning("No 'text' column found, using all available columns")
            full_text_df = articles_to_use.copy()
    mark_step(
        "full_text_selection_end",
        meta={"rows": len(full_text_df), "columns": len(full_text_df.columns)},
    )

    logger.info(f"Prepared full text data with {len(full_text_df)} articles and {len(full_text_df.columns)} columns")

    # Step 3: Export to Backblaze B2 as CSV
    slug = create_url_safe_slug(params.query)
    object_name = (
        f"{params.b2_object_prefix}/DATE/{slug}-full-text.csv"
    )
    logger.info(f"Exporting full text to B2: {object_name}")
    
    b2_metadata, b2_artifact = csv_to_b2(
        full_text_df,
        object_name=object_name,
        add_date_slug=params.b2_add_date_slug,
        ensure_unique=params.b2_ensure_unique,
    )

    # Step 4: Send email notification if recipients are specified
    if params.email_to:
        send_run_summary_email(
            email_to=params.email_to,
            query_summary=query_summary,
            b2_artifact=b2_artifact,
            flow_name="full_text_download",
            query=params.query,
        )

    # Return FlowOutput model instance - these are saved as Prefect artifacts
    return FullTextDownloadFlowOutput(
        query_summary=query_summary,
        b2_artifact=b2_artifact,
    )
