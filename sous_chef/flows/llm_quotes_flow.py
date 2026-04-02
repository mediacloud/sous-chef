from __future__ import annotations

"""
Demo flow: query MediaCloud and summarize articles with an LLM task.
"""

from pydantic import Field

from ..flow import register_flow, BaseFlowOutput
from ..runtime import mark_step
from ..params.mediacloud_query import MediacloudQuery
from ..params.csv_export import CsvExportParams
from ..params.email_recipient import EmailRecipientParam
from ..params.llm_params import GroqModelParams
from ..params.webhook_callback import WebhookCallbackParam
from ..artifacts import MediacloudQuerySummary, FileUploadArtifact, LLMCostSummary
from ..tasks.discovery_tasks import query_online_news
from ..tasks.llm_quotes import article_quotes_llm
from ..tasks.export_tasks import csv_to_b2
from ..tasks.email_tasks import send_run_summary_email
from ..utils import create_url_safe_slug, get_logger


class LLMQuotesFlowParams(MediacloudQuery, GroqModelParams, CsvExportParams, EmailRecipientParam, WebhookCallbackParam):

    max_articles: int = Field(
        default=1,
        description="Maximum number of articles to analyze (for demos).",
    )


class LLMQuotesFlowOutput(BaseFlowOutput):
    """Output artifacts for the LLM quotes flow."""
    query_summary: MediacloudQuerySummary
    b2_artifact: FileUploadArtifact
    llm_cost: LLMCostSummary


@register_flow(
    name="llm_quotes",
    description=(
        "Query MediaCloud and extract quotes and speakers with a structured LLM task."
    ),
    params_model=LLMQuotesFlowParams,
    output_model=LLMQuotesFlowOutput,
    log_prints=True,
)
def llm_quotes_flow(params: LLMQuotesFlowParams) -> LLMQuotesFlowOutput:
    logger = get_logger()
    logger.info("starting llm quotes flow")

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

    # Step 2: Limit articles for demo
    if params.max_articles and params.max_articles > 0:
        articles = articles.head(params.max_articles).copy()

    # Step 3: Run task
    mark_step("llm_quotes_start", meta={"articles": len(articles)})
    quotes_df, cost_summary = article_quotes_llm(
        articles,
        text_col="text",
        # model_name=params.model_name,
    )
    mark_step("llm_quotes_end", meta={"quotes": len(quotes_df)})

    # Step 4: Prepare export data (remove full text column)
    # export_df = articles_with_summaries.drop(columns=["text"], errors="ignore").copy()

    # Step 5: Export results to Backblaze B2 as CSV
    slug = create_url_safe_slug(params.query)
    object_name = (
        f"{params.b2_object_prefix}/DATE/{slug}-llm-quotes.csv"
    )
    logger.info(f"Exporting LLM quotes to B2: {object_name}")
    b2_metadata, b2_artifact = csv_to_b2(
        quotes_df,
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
            flow_name="llm_quotes",
            query=params.query,
        )

    # Return FlowOutput model instance - these are saved as Prefect artifacts
    return LLMQuotesFlowOutput(
        query_summary=query_summary,
        b2_artifact=b2_artifact,
        llm_cost=cost_summary,
    )

