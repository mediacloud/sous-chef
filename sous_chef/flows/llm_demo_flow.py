from __future__ import annotations

"""
Demo flow: query MediaCloud and summarize articles with an LLM task.
"""

from typing import Any, Dict

from pydantic import Field

from ..flow import register_flow
from ..params.mediacloud_query import MediacloudQuery
from ..params.csv_export import CsvExportParams
from ..params.email_recipient import EmailRecipientParam
from ..tasks.discovery_tasks import query_online_news
from ..tasks.llm_article_summary import summarize_articles_llm
from ..tasks.export_tasks import csv_to_b2
from ..tasks.email_tasks import send_run_summary_email
from ..utils import create_url_safe_slug, get_logger


class LLMDemoFlowParams(MediacloudQuery, CsvExportParams, EmailRecipientParam):
    """
    Parameters for the LLM demo flow.
    """

    model_name: str = Field(
        default="meta-llama/Meta-Llama-3-8B-Instruct",
        description="Model identifier to use via LiteLLM.",
    )
    max_articles: int = Field(
        default=20,
        description="Maximum number of articles to summarize (for demos).",
    )


@register_flow(
    name="llm_demo",
    description=(
        "Demo: Query MediaCloud and summarize articles with a structured LLM task."
    ),
    params_model=LLMDemoFlowParams,
    log_prints=True,
)
def llm_demo_flow(params: LLMDemoFlowParams) -> Dict[str, Any]:
    """
    Demo flow that:
      1. Queries MediaCloud for articles matching a query.
      2. Runs an LLM-based summarization task over a subset of articles.
      3. Exports results to B2 and sends email notification.
    
    Args:
        params: Flow parameters
        
    Returns:
        Dictionary containing artifact objects:
        - query_summary: MediacloudQuerySummary artifact with query context and statistics
        - b2_artifact: FileUploadArtifact with upload details for the exported CSV
    """
    logger = get_logger()
    logger.info("starting llm demo flow")

    # Step 1: Query MediaCloud for articles
    articles, query_summary = query_online_news(
        query=params.query,
        collection_ids=params.collection_ids,
        source_ids=params.source_ids,
        start_date=params.start_date,
        end_date=params.end_date,
    )

    # Step 2: Limit articles for demo
    if params.max_articles and params.max_articles > 0:
        articles = articles.head(params.max_articles).copy()

    # Step 3: Run LLM summarization task
    articles_with_summaries = summarize_articles_llm(
        articles,
        text_col="text",
        title_col="title",
        model_name=params.model_name,
    )

    # Step 4: Prepare export data (remove full text column)
    export_df = articles_with_summaries.drop(columns=["text"], errors="ignore").copy()

    # Step 5: Export results to Backblaze B2 as CSV
    slug = create_url_safe_slug(params.query)
    object_name = (
        f"{params.b2_object_prefix}/DATE/{slug}-llm-summaries.csv"
    )
    logger.info(f"Exporting LLM summaries to B2: {object_name}")
    b2_metadata, b2_artifact = csv_to_b2(
        export_df,
        object_name=object_name,
        add_date_slug=params.b2_add_date_slug,
        ensure_unique=params.b2_ensure_unique,
    )

    # Step 6: Send email notification if recipients are specified
    if params.email_to:
        email_result = send_run_summary_email(
            email_to=params.email_to,
            query_summary=query_summary,
            b2_artifact=b2_artifact,
            flow_name="llm_demo",
            query=params.query,
        )

    # Return only artifact objects - these are saved as Prefect artifacts
    return {
        "query_summary": query_summary,
        "b2_artifact": b2_artifact,
    }

