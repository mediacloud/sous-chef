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
from ..tasks.image_tasks import top_image
from ..tasks.export_tasks import csv_to_b2
from ..tasks.email_tasks import send_run_summary_email
from ..utils import create_url_safe_slug


class TopImageParams(MediacloudQuery, CsvExportParams, EmailRecipientParam, WebhookCallbackParam):
    """Parameters for flow."""


class TopImageFlowOutput(BaseFlowOutput):
    """Output artifacts for the top image flow."""
    query_summary: MediacloudQuerySummary
    b2_artifact: FileUploadArtifact


@register_flow(
    name="top_images",
    description="Identify top image URLs for each story",
    params_model=TopImageParams,
    output_model=TopImageFlowOutput,
    log_prints=True
)
def top_images_flow(params: TopImageParams) -> TopImageFlowOutput:
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
    deduplicated_articles_df = deduplicate_on_title_source(
        articles,
    )
    mark_step("title_source_dedup_end", meta={"articles": len(deduplicated_articles_df)})

    # Step 3: Add in top image info (gotta fetch html)
    mark_step("html_fetch_parse_start", meta={"rows": len(deduplicated_articles_df)})
    articles_with_image_info_df = top_image(
        deduplicated_articles_df,
    )
    mark_step("html_fetch_parse_end", meta={"rows": len(articles_with_image_info_df)})

    # Optional Step 4: Export to Backblaze B2 as CSV
    export_df = articles_with_image_info_df.drop(columns=["text"], errors="ignore").copy()
    slug = create_url_safe_slug(params.query)
    object_name = (
            f"{params.b2_object_prefix}/DATE/{slug}-top-images.csv"
    )
    print(f"Exporting data to B2: {object_name}")
    b2_metadata, b2_artifact = csv_to_b2(
            export_df,
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
            flow_name="top_images",
            query=params.query
        )
    
    # Return FlowOutput model instance - these are saved as Prefect artifacts
    return TopImageFlowOutput(
        query_summary=query_summary,
        b2_artifact=b2_artifact,
    )
