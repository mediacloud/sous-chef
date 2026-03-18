"""
Flow: query -> LLM aboutness filter -> LLM summaries -> CSV export to B2.

This flow intentionally exports a "core story metadata + aboutness + summaries"
CSV (no full `text` column).
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
from pydantic import Field

from ..flow import register_flow, BaseFlowOutput
from ..params import (
    MediacloudQuery,
    CsvExportParams,
    WebhookCallbackParam,
    GroqModelParams,
)
from ..params.mediacloud_query import DedupStrategy
from ..params.aboutness import AboutnessParams, AboutnessTargetKind, build_default_about_context
from ..artifacts import (
    MediacloudQuerySummary,
    FileUploadArtifact,
    LLMCostSummary,
    AboutnessFilterSummary,
)
from ..tasks import (
    query_online_news,
    score_aboutness_llm,
    summarize_articles_llm,
    csv_to_b2,
)
from ..utils import create_url_safe_slug, get_logger


class AboutnessFilteredSummariesParams(
    MediacloudQuery,
    AboutnessParams,
    GroqModelParams,
    CsvExportParams,
    WebhookCallbackParam,
):
    """
    Parameters for the aboutness-filtered summaries flow.

    This flow enforces deduplication on story title by passing
    `DedupStrategy.title` into `query_online_news()`.
    """

    # Keep the default in the UI/schema aligned with the enforcement.
    dedup_strategy: DedupStrategy = Field(
        default=DedupStrategy.title,
        title="Deduplication strategy",
        description=(
            "For this flow, duplicates are deduplicated by story title "
            "(title-based dedup)."
        ),
    )

    aboutness_threshold: float = 0.5
    # Cap how many articles are sent to each LLM step (aboutness, then summarization).
    max_articles_per_step: Optional[int] = None


class AboutnessFilteredSummariesFlowOutput(BaseFlowOutput):
    """Output artifacts for the aboutness-filtered summaries flow."""

    query_summary: MediacloudQuerySummary
    filter_summary: AboutnessFilterSummary
    aboutness_llm_cost: LLMCostSummary
    summarizer_llm_cost: LLMCostSummary
    b2_artifact: FileUploadArtifact


_EXPORT_COLUMNS: list[str] = [
    # Core metadata (best-effort: included if present in the MediaCloud frame)
    "stories_id",
    "title",
    "url",
    "publish_date",
    "media_name",
    "media_id",
    "language",
    # Aboutness judge outputs
    "about_score",
    "about_is_about",
    "about_reason",
    "about_error",
    # Summarizer outputs
    "llm_summary_text",
    "llm_summary_is_confident",
    "llm_summary_error",
    # Placeholder for downstream zero-shot classifier integration.
    # TODO: populate this column once the zero-shot classifier task exists.
    "zero-shot-tags",
]


@register_flow(
    name="aboutness_filtered_summaries",
    description=(
        "Query MediaCloud, deduplicate by story title, score/filter with the "
        "LLM aboutness judge, summarize with the LLM summarizer, and upload "
        "a core-metadata CSV (no full text) to B2. "
        "Includes a placeholder `zero-shot-tags` column for future "
        "zero-shot classification (TODO: populate when the classifier task "
        "is available)."
    ),
    params_model=AboutnessFilteredSummariesParams,
    output_model=AboutnessFilteredSummariesFlowOutput,
    log_prints=True,
)
def aboutness_filtered_summaries_flow(
    params: AboutnessFilteredSummariesParams,
) -> AboutnessFilteredSummariesFlowOutput:
    logger = get_logger()
    logger.info("starting aboutness_filtered_summaries flow")

    # Step 1: Query MediaCloud for articles (title-based dedup enforced)
    articles, query_summary = query_online_news(
        query=params.query,
        collection_ids=params.collection_ids,
        source_ids=params.source_ids,
        start_date=params.start_date,
        end_date=params.end_date,
        dedup_strategy=DedupStrategy.title,
        upload_dedup_summary=params.upload_dedup_summary,
    )

    # Stable empty-case upload contract
    if articles.empty:
        bins = [i / 10 for i in range(11)]
        empty_df = pd.DataFrame(columns=_EXPORT_COLUMNS)

        slug = create_url_safe_slug(params.query or params.about_target)
        object_name = (
            f"{params.b2_object_prefix}/DATE/{slug}-aboutness-filtered-summaries.csv"
        )

        _, b2_artifact = csv_to_b2(
            empty_df,
            object_name=object_name,
            add_date_slug=params.b2_add_date_slug,
            ensure_unique=params.b2_ensure_unique,
        )

        filter_summary = AboutnessFilterSummary(
            total_scored=0,
            passed_count=0,
            discarded_count=0,
            threshold=params.aboutness_threshold,
            score_histogram_counts=[0] * 10,
            score_histogram_edges=bins,
            target_kind=params.about_target_kind.value,
        )

        empty_cost = LLMCostSummary.from_groq_summaries(
            model=params.model_name,
            usages=[],
        )

        return AboutnessFilteredSummariesFlowOutput(
            query_summary=query_summary,
            filter_summary=filter_summary,
            aboutness_llm_cost=empty_cost,
            summarizer_llm_cost=empty_cost,
            b2_artifact=b2_artifact,
        )

    max_rows = params.max_articles_per_step

    # Step 2: Build aboutness context (mirrors `aboutness_filter_flow`)
    if params.about_target_kind == AboutnessTargetKind.custom:
        context = params.about_context or None
    else:
        context = build_default_about_context(
            params.about_target_kind,
            params.about_target,
        )

    # Step 3: LLM aboutness scoring
    scored_df, aboutness_cost = score_aboutness_llm(
        articles,
        target=params.about_target,
        context=context,
        text_col="text",
        title_col="title",
        model_name=params.model_name,
        max_rows=max_rows,
    )

    # Step 4: Filter by aboutness threshold
    threshold = params.aboutness_threshold
    filtered_df = scored_df[
        (scored_df["about_is_about"] == True)  # noqa: E712
        & (scored_df["about_score"] >= threshold)
    ].copy()

    total_scored = len(scored_df)
    passed_count = len(filtered_df)
    discarded_count = total_scored - passed_count

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
    )

    logger.info(
        "aboutness_filtered_summaries: %d of %d passed threshold %.2f",
        passed_count,
        total_scored,
        threshold,
    )

    # Step 5: LLM summarization over the filtered subset
    summarized_df, summarizer_cost = summarize_articles_llm(
        filtered_df,
        text_col="text",
        title_col="title",
        model_name=params.model_name,
        max_rows=max_rows,
    )

    # Step 6: Export (no full text)
    export_df = summarized_df.drop(columns=["text"], errors="ignore").copy()

    # Ensure stable column contract/order for downstream CSV ingestion.
    for col in _EXPORT_COLUMNS:
        if col not in export_df.columns:
            export_df[col] = None
    export_df = export_df[_EXPORT_COLUMNS]

    slug = create_url_safe_slug(params.query or params.about_target)
    object_name = (
        f"{params.b2_object_prefix}/DATE/{slug}-aboutness-filtered-summaries.csv"
    )

    _, b2_artifact = csv_to_b2(
        export_df,
        object_name=object_name,
        add_date_slug=params.b2_add_date_slug,
        ensure_unique=params.b2_ensure_unique,
    )

    return AboutnessFilteredSummariesFlowOutput(
        query_summary=query_summary,
        filter_summary=filter_summary,
        aboutness_llm_cost=aboutness_cost,
        summarizer_llm_cost=summarizer_cost,
        b2_artifact=b2_artifact,
    )

