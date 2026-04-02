"""
Flow: query -> LLM aboutness filter -> LLM summaries -> CSV export to B2.

This flow intentionally exports a "core story metadata + aboutness + summaries"
CSV (no full `text` column).
"""

from __future__ import annotations

import json
from typing import Optional

import pandas as pd
from pydantic import Field

from ..flow import register_flow, BaseFlowOutput
from ..runtime import mark_step
from ..params import (
    MediacloudQuery,
    CsvExportParams,
    WebhookCallbackParam,
    GroqModelParams,
)
from ..params.mediacloud_query import DedupStrategy
from ..params.aboutness import AboutnessParams, AboutnessTargetKind, build_default_about_context
from ..params.zeroshot import ZeroShotClassificationParams
from ..artifacts import (
    MediacloudQuerySummary,
    FileUploadArtifact,
    LLMCostSummary,
    AboutnessFilterSummary,
    ZeroShotClassificationSummary,
)
from ..tasks import (
    query_online_news,
    score_aboutness_llm,
    summarize_articles_llm,
    csv_to_b2,
    DEFAULT_ZEROSHOT_MODEL,
    ZEROSHOT_CLASSIFY_DEVICE,
    ZEROSHOT_STORY_TEXT_COLUMN,
    compute_zero_shot_label_counts,
    zeroshot_classification_failure_details,
    zero_shot_classify_stories,
)
from ..utils import create_url_safe_slug, get_logger


class TaggedFilteredSummariesParams(
    MediacloudQuery,
    AboutnessParams,
    ZeroShotClassificationParams,
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
    store_filter_scored_rows: bool = Field(
        default = False,
        title="Store aboutness-scored rows (before filtering)",
        description=(
            "For this flow, store a csv with the whole mc query result before filtering for aboutness")
        )
    aboutness_threshold: float = 0.5
    # Cap how many articles are sent to each LLM step (aboutness, then summarization).
    max_articles_per_step: Optional[int] = None


class TaggedFilteredSummariesFlowOutput(BaseFlowOutput):
    """Output artifacts for the tagged-filtered summaries flow."""

    query_summary: MediacloudQuerySummary
    filter_summary: AboutnessFilterSummary
    aboutness_llm_cost: LLMCostSummary
    summarizer_llm_cost: LLMCostSummary
    zeroshot_summary: ZeroShotClassificationSummary
    b2_artifact: FileUploadArtifact
    unfiltered_b2_artifact: FileUploadArtifact


_EXPORT_COLUMNS: list[str] = [
    # Core metadata (best-effort: included if present in the MediaCloud frame)
    "story_id",
    "title",
    "url",
    "publish_date",
    "media_name",
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
    # Zero-shot matching labels for each row (JSON list as string).
    "zero-shot-tags",
    "zeroshot_error",
]


@register_flow(
    name="tagged_filtered_summaries",
    description=(
        "Query MediaCloud, deduplicate by story title, filter with the LLM "
        "aboutness judge, summarize with the LLM summarizer, then add "
        "zero-shot tags and upload a core-metadata CSV (no full text) to B2."
    ),
    params_model=TaggedFilteredSummariesParams,
    output_model=TaggedFilteredSummariesFlowOutput,
    log_prints=True,
)
def tagged_filtered_summaries_flow(
    params: TaggedFilteredSummariesParams,
) -> TaggedFilteredSummariesFlowOutput:
    logger = get_logger()
    logger.info("starting tagged_filtered_summaries flow")

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

    if params.max_stories is not None:
        articles = articles.head(params.max_stories).copy()

    # Stable empty-case upload contract
    if articles.empty:
        bins = [i / 10 for i in range(11)]
        empty_df = pd.DataFrame(columns=_EXPORT_COLUMNS)

        slug = create_url_safe_slug(params.query or params.about_target)
        object_name = (
            f"{params.b2_object_prefix}/DATE/{slug}-tagged-filtered-summaries.csv"
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
            about_target=params.about_target,
        )

        empty_cost = LLMCostSummary.from_groq_summaries(
            model=params.model_name,
            usages=[],
        )
        zeroshot_summary = ZeroShotClassificationSummary(
            input_labels=params.classification_labels,
            label_counts=[0] * len(params.classification_labels),
            stories_classified=0,
            stories_without_prediction=0,
            stories_classification_failed=0,
            classification_failure_details=[],
            summary_score_threshold=params.zeroshot_score_threshold,
            distribution_mode=(
                "threshold_ge"
                if params.zeroshot_score_threshold is not None
                else "top_label"
            ),
            multi_label=params.multi_label,
            hypothesis_template=params.hypothesis_template,
            model_id=DEFAULT_ZEROSHOT_MODEL,
        )

        return TaggedFilteredSummariesFlowOutput(
            query_summary=query_summary,
            filter_summary=filter_summary,
            aboutness_llm_cost=empty_cost,
            summarizer_llm_cost=empty_cost,
            zeroshot_summary=zeroshot_summary,
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
    mark_step("aboutness_scoring_start", meta={"articles": len(articles)})
    scored_df, aboutness_cost = score_aboutness_llm(
        articles,
        target=params.about_target,
        context=context,
        text_col="text",
        title_col="title",
        model_name=params.model_name,
        max_rows=max_rows,
    )
    mark_step("aboutness_scoring_end", meta={"articles": len(scored_df)})

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
        about_target=params.about_target,
    )

    logger.info(
        "tagged_filtered_summaries: %d of %d passed threshold %.2f",
        passed_count,
        total_scored,
        threshold,
    )

    # Step 5: LLM summarization over the filtered subset
    mark_step("llm_summarization_start", meta={"articles": len(filtered_df)})
    summarized_df, summarizer_cost = summarize_articles_llm(
        filtered_df,
        text_col="text",
        title_col="title",
        model_name=params.model_name,
        max_rows=max_rows,
    )
    mark_step("llm_summarization_end", meta={"articles": len(summarized_df)})

    # Step 6: Zero-shot classification on original article text for filtered rows.
    mark_step(
        "zeroshot_classification_start",
        meta={
            "stories": len(summarized_df),
            "labels": len(params.classification_labels),
            "multi_label": params.multi_label,
        },
    )
    if summarized_df.empty:
        classified_df = summarized_df.copy()
    else:
        classified_df = zero_shot_classify_stories(
            summarized_df,
            params.classification_labels,
            text_column=ZEROSHOT_STORY_TEXT_COLUMN,
            hypothesis_template=params.hypothesis_template,
            multi_label=params.multi_label,
            model=DEFAULT_ZEROSHOT_MODEL,
            device=ZEROSHOT_CLASSIFY_DEVICE,
            passing_score_threshold=params.zeroshot_score_threshold,
        )
    label_counts, stories_without_prediction, stories_failed = (
        compute_zero_shot_label_counts(
            classified_df,
            params.classification_labels,
            params.zeroshot_score_threshold,
        )
    )
    failure_details = zeroshot_classification_failure_details(classified_df)
    mark_step(
        "zeroshot_classification_end",
        meta={
            "stories": len(classified_df),
            "classification_failures": stories_failed,
        },
    )

    zeroshot_summary = ZeroShotClassificationSummary(
        input_labels=params.classification_labels,
        label_counts=label_counts,
        stories_classified=len(classified_df),
        stories_without_prediction=stories_without_prediction,
        stories_classification_failed=stories_failed,
        classification_failure_details=failure_details,
        summary_score_threshold=params.zeroshot_score_threshold,
        distribution_mode=(
            "threshold_ge"
            if params.zeroshot_score_threshold is not None
            else "top_label"
        ),
        multi_label=params.multi_label,
        hypothesis_template=params.hypothesis_template,
        model_id=DEFAULT_ZEROSHOT_MODEL,
    )

    # Populate placeholder output column with matching zero-shot labels.
    if params.zeroshot_score_threshold is not None:
        classified_df["zero-shot-tags"] = classified_df.get(
            "zeroshot_labels_passing_threshold_json", "[]"
        )
    else:
        tags_col: list[str] = []
        for label in classified_df.get("zeroshot_top_label", []):
            if isinstance(label, str) and label.strip():
                tags_col.append(json.dumps([label]))
            else:
                tags_col.append("[]")
        classified_df["zero-shot-tags"] = tags_col

    # Step 7: Export (no full text)
    export_df = classified_df.drop(columns=["text"], errors="ignore").copy()

    # Ensure stable column contract/order for downstream CSV ingestion.
    for col in _EXPORT_COLUMNS:
        if col not in export_df.columns:
            export_df[col] = None
    export_df = export_df[_EXPORT_COLUMNS]

    slug = create_url_safe_slug(params.query or params.about_target)
    object_name = (
        f"{params.b2_object_prefix}/DATE/{slug}-tagged-filtered-summaries.csv"
    )


    _, b2_artifact = csv_to_b2(
        export_df,
        object_name=object_name,
        add_date_slug=params.b2_add_date_slug,
        ensure_unique=params.b2_ensure_unique,
    )

    scored_object_name = (
        f"{params.b2_object_prefix}/DATE/{slug}-tagged-unfiltered-summaries.csv"
    )

    if params.store_filter_scored_rows:
        _, unfiltered_b2_artifact = csv_to_b2(
            scored_df,
            object_name = scored_object_name,
            add_date_slug=params.b2_add_date_slug,
            ensure_unique=params.b2_ensure_unique,
            )
    else:
        unfiltered_b2_artifact = FileUploadArtifact(bucket="", object_key="")




    return TaggedFilteredSummariesFlowOutput(
        query_summary=query_summary,
        filter_summary=filter_summary,
        aboutness_llm_cost=aboutness_cost,
        summarizer_llm_cost=summarizer_cost,
        zeroshot_summary=zeroshot_summary,
        b2_artifact=b2_artifact,
        unfiltered_b2_artifact=unfiltered_b2_artifact
    )

