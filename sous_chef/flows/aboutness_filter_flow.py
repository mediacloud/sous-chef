"""
Simple flow: apply LLM aboutness scoring to articles from a MediaCloud query.

This demonstrates how to:
- Query MediaCloud for articles
- Score how much each article is about a target subject
- Filter by an aboutness threshold
- Optionally export filtered results to B2 and send an email
"""

from typing import Optional

import pandas as pd

from ..flow import register_flow, BaseFlowOutput
from ..runtime import mark_step
from ..params import (
    MediacloudQuery,
    CsvExportParams,
    EmailRecipientParam,
    WebhookCallbackParam,
    GroqModelParams,
    AboutnessParams,
)
from ..params.aboutness import AboutnessTargetKind, build_default_about_context
from ..artifacts import (
    MediacloudQuerySummary,
    FileUploadArtifact,
    LLMCostSummary,
    AboutnessFilterSummary,
)
from ..tasks import (
    query_online_news,
    csv_to_b2,
    send_run_summary_email,
    score_aboutness_llm,
)
from ..utils import create_url_safe_slug, get_logger


class AboutnessFilterParams(
    MediacloudQuery,
    CsvExportParams,
    EmailRecipientParam,
    WebhookCallbackParam,
    GroqModelParams,
    AboutnessParams,
):
    """
    Parameters for the aboutness filter flow.
    """

    aboutness_threshold: float = 0.5
    max_articles_for_llm: Optional[int] = None


class AboutnessFilterFlowOutput(BaseFlowOutput):
    """
    Output artifacts for the aboutness filter flow.
    """

    query_summary: MediacloudQuerySummary
    filter_summary: AboutnessFilterSummary
    llm_cost: LLMCostSummary
    scored_b2_artifact: FileUploadArtifact
    filtered_b2_artifact: FileUploadArtifact


@register_flow(
    name="aboutness_filter",
    description=(
        "Apply an LLM aboutness judge to articles from a MediaCloud query, "
        "filtering to those substantially about a target subject."
    ),
    params_model=AboutnessFilterParams,
    output_model=AboutnessFilterFlowOutput,
    log_prints=True,
)
def aboutness_filter_flow(params: AboutnessFilterParams) -> AboutnessFilterFlowOutput:
    """
    Apply LLM aboutness scoring to articles from a MediaCloud query.

    Steps:
      1. Query MediaCloud for articles matching the query.
      2. Run the generic LLM aboutness judge over articles.
      3. Filter articles by an aboutness threshold.
      4. Export filtered results to B2 and return artifacts.
    """

    logger = get_logger()
    logger.info("starting aboutness filter flow")

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
    mark_step("mediacloud_query_complete")

    if articles.empty:
        logger.info("no articles returned from MediaCloud; skipping aboutness scoring")
        # Still export empty CSVs to keep the pattern consistent
        empty_df = pd.DataFrame()
        slug = create_url_safe_slug(params.query or "aboutness")
        base_object_prefix = f"{params.b2_object_prefix}/DATE/{slug}-aboutness"

        # Unfiltered (scored) CSV – empty in this case
        object_name_scored = f"{base_object_prefix}-scored.csv"
        _, scored_b2_artifact = csv_to_b2(
            empty_df,
            object_name=object_name_scored,
            add_date_slug=params.b2_add_date_slug,
            ensure_unique=params.b2_ensure_unique,
        )

        # Filtered CSV – also empty
        object_name_filtered = f"{base_object_prefix}-filtered.csv"
        _, filtered_b2_artifact = csv_to_b2(
            empty_df,
            object_name=object_name_filtered,
            add_date_slug=params.b2_add_date_slug,
            ensure_unique=params.b2_ensure_unique,
        )

        # Build an empty histogram with 10 bins over [0.0, 1.0]
        bins = [i / 10 for i in range(11)]
        filter_summary = AboutnessFilterSummary(
            total_scored=0,
            passed_count=0,
            discarded_count=0,
            threshold=params.aboutness_threshold,
            score_histogram_counts=[0] * 10,
            score_histogram_edges=bins,
            target_kind=params.about_target_kind.value,
            about_target=params.about_target,
        )
        llm_cost = LLMCostSummary.from_groq_summaries(
            model=params.model_name,
            usages=[],
        )
        return AboutnessFilterFlowOutput(
            query_summary=query_summary,
            filter_summary=filter_summary,
            llm_cost=llm_cost,
            scored_b2_artifact=scored_b2_artifact,
            filtered_b2_artifact=filtered_b2_artifact,
        )

    # Optional: limit how many articles we send to the LLM
    max_rows = params.max_articles_for_llm

    # Step 2: Apply LLM aboutness scoring
    # When target kind is Custom, use the user's about_context; otherwise use
    # the preset for the selected kind (and ignore about_context).
    if params.about_target_kind == AboutnessTargetKind.custom:
        context = params.about_context or None
    else:
        context = build_default_about_context(params.about_target_kind, params.about_target)

    scored_df, cost_summary = score_aboutness_llm(
        articles,
        target=params.about_target,
        context=context,
        text_col="text",
        title_col="title",
        model_name=params.model_name,
        max_rows=max_rows,
    )

    # Step 3: Filter by aboutness threshold
    threshold = params.aboutness_threshold
    filtered_df = scored_df[
        (scored_df["about_is_about"] == True)  # noqa: E712
        & (scored_df["about_score"] >= threshold)
    ].copy()

    total_scored = len(scored_df)
    passed_count = len(filtered_df)
    discarded_count = total_scored - passed_count

    logger.info(
        "aboutness filter: %d of %d articles passed threshold %.2f",
        passed_count,
        total_scored,
        threshold,
    )

    # Build a 10-bin histogram of about_score over [0.0, 1.0]
    bins = [i / 10 for i in range(11)]
    scores = scored_df["about_score"].dropna()
    if scores.empty:
        hist_counts = [0] * 10
    else:
        binned = pd.cut(scores, bins=bins, include_lowest=True)
        # Ensure stable bin order
        hist_counts = binned.value_counts().sort_index().tolist()

    filter_summary = AboutnessFilterSummary(
        total_scored=total_scored,
        passed_count=passed_count,
        discarded_count=discarded_count,
        threshold=threshold,
        score_histogram_counts=hist_counts,
        score_histogram_edges=bins,
        target_kind=params.about_target_kind.value,
        about_target=params.about_target,
    )

    # Step 4: Export scored and filtered results to B2 as CSV
    slug = create_url_safe_slug(params.query or params.about_target)
    base_object_prefix = f"{params.b2_object_prefix}/DATE/{slug}-aboutness"

    # 4a: Unfiltered, scored articles (drop raw text for export)
    scored_export_df = scored_df.drop(columns=["text"], errors="ignore").copy()
    object_name_scored = f"{base_object_prefix}-scored.csv"
    _, scored_b2_artifact = csv_to_b2(
        scored_export_df,
        object_name=object_name_scored,
        add_date_slug=params.b2_add_date_slug,
        ensure_unique=params.b2_ensure_unique,
    )

    # 4b: Filtered subset
    object_name_filtered = f"{base_object_prefix}-filtered.csv"
    _, filtered_b2_artifact = csv_to_b2(
        filtered_df,
        object_name=object_name_filtered,
        add_date_slug=params.b2_add_date_slug,
        ensure_unique=params.b2_ensure_unique,
    )

    # Optional: send email notification
    if params.email_to:
        send_run_summary_email(
            email_to=params.email_to,
            query_summary=query_summary,
            b2_artifact=filtered_b2_artifact,
            flow_name="aboutness_filter",
            query=params.query,
        )

    return AboutnessFilterFlowOutput(
        query_summary=query_summary,
        filter_summary=filter_summary,
        llm_cost=cost_summary,
        scored_b2_artifact=scored_b2_artifact,
        filtered_b2_artifact=filtered_b2_artifact,
    )

