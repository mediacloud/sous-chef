"""
Flow: query -> LLM aboutness filter -> zero-shot tags -> LLM summaries -> CSV export to B2.

Zero-shot runs before summarization so predicted labels can steer the summarizer.
This flow exports a "core story metadata + aboutness + summaries + tags"
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
from ..tasks.zeroshot import build_zero_shot_tag_scores_json_for_row
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
    aboutness_threshold: float = 0.5
    # Cap how many articles are sent to each LLM step (aboutness, zeroshot, summarization).
    max_articles_per_step: Optional[int] = None
    summary_focus_context: Optional[str] = Field(
        default=None,
        title="Summarizer run focus (optional)",
        description=(
            "Optional themes for the LLM summarizer for every article in the batch. "
            "If unset, the aboutness target is used. Set to an empty string to omit "
            "run-level focus (per-article zeroshot tags may still apply)."
        ),
    )


def _resolve_tagged_flow_summary_focus(
    params: TaggedFilteredSummariesParams,
) -> Optional[str]:
    """
    Run-level string passed into the article summarizer.

    ``summary_focus_context`` None → use ``about_target``. Explicit empty string → no run focus.
    """
    if params.summary_focus_context is None:
        s = (params.about_target or "").strip()
        return s or None
    s = params.summary_focus_context.strip()
    return s or None


class TaggedFilteredSummariesFlowOutput(BaseFlowOutput):
    """Output artifacts for the tagged-filtered summaries flow."""

    query_summary: MediacloudQuerySummary
    filter_summary: AboutnessFilterSummary
    aboutness_llm_cost: LLMCostSummary
    summarizer_llm_cost: LLMCostSummary
    zeroshot_summary: ZeroShotClassificationSummary
    b2_artifact: FileUploadArtifact
    # Populated from score_aboutness_llm when upload_prefiltered_rows is enabled.
    prefiltered_b2_artifact: FileUploadArtifact


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
    # Scores for those labels only: JSON object mapping label -> float (or null for top-label mode if score missing).
    "zero-shot-tag-scores",
    "zeroshot_error",
]


@register_flow(
    name="tagged_filtered_summaries",
    description=(
        "Query MediaCloud, deduplicate by story title, filter with the LLM "
        "aboutness judge, run zero-shot tagging on article text, summarize with "
        "the LLM (using aboutness target and predicted tags as optional focus), "
        "then upload a core-metadata CSV (no full text) to B2."
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
            prefiltered_b2_artifact=FileUploadArtifact(bucket="", object_key=""),
        )

    max_rows = params.max_articles_per_step

    slug = create_url_safe_slug(params.query or params.about_target)
    scored_object_name = (
        f"{params.b2_object_prefix}/DATE/{slug}-tagged-unfiltered-summaries.csv"
    )

    # Step 2: Build aboutness context (mirrors `aboutness_filter_flow`)
    if params.about_target_kind == AboutnessTargetKind.custom:
        context = params.about_context or None
    else:
        context = build_default_about_context(
            params.about_target_kind,
            params.about_target,
        )

    # Step 3: LLM aboutness scoring (optional B2 upload of all scored rows, task-level)
    mark_step("aboutness_scoring_start", meta={"articles": len(articles)})
    scored_df, aboutness_run = score_aboutness_llm(
        articles,
        target=params.about_target,
        context=context,
        text_col="text",
        title_col="title",
        model_name=params.model_name,
        max_rows=max_rows,
        upload_scored_rows_csv=params.upload_prefiltered_rows,
        scored_csv_object_name=(
            scored_object_name if params.upload_prefiltered_rows else None
        ),
        b2_add_date_slug=params.b2_add_date_slug,
        b2_ensure_unique=params.b2_ensure_unique,
        drop_text_for_scored_csv=False,
    )
    aboutness_cost = aboutness_run.llm_cost
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

    # Step 5: Zero-shot on article text (before summarization so tags steer the LLM).
    mark_step(
        "zeroshot_classification_start",
        meta={
            "stories": len(filtered_df),
            "labels": len(params.classification_labels),
            "multi_label": params.multi_label,
        },
    )
    if filtered_df.empty:
        classified_df = filtered_df.copy()
    else:
        classified_df = zero_shot_classify_stories(
            filtered_df,
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

    # Step 6: LLM summarization (run focus + per-row zeroshot tags when present).
    mark_step("llm_summarization_start", meta={"articles": len(classified_df)})
    summarized_df, summarizer_cost = summarize_articles_llm(
        classified_df,
        text_col="text",
        title_col="title",
        model_name=params.model_name,
        max_rows=max_rows,
        focus_context=_resolve_tagged_flow_summary_focus(params),
        use_zeroshot_row_tags=True,
    )
    mark_step("llm_summarization_end", meta={"articles": len(summarized_df)})

    # Populate output column with matching zero-shot labels for CSV export.
    use_passing_threshold = params.zeroshot_score_threshold is not None
    if use_passing_threshold:
        summarized_df["zero-shot-tags"] = summarized_df.get(
            "zeroshot_labels_passing_threshold_json", "[]"
        )
    else:
        tags_col: list[str] = []
        for label in summarized_df.get("zeroshot_top_label", []):
            if isinstance(label, str) and label.strip():
                tags_col.append(json.dumps([label]))
            else:
                tags_col.append("[]")
        summarized_df["zero-shot-tags"] = tags_col

    summarized_df["zero-shot-tag-scores"] = [
        build_zero_shot_tag_scores_json_for_row(
            row,
            use_passing_threshold=use_passing_threshold,
        )
        for _, row in summarized_df.iterrows()
    ]

    # Step 7: Export (no full text)
    export_df = summarized_df.drop(columns=["text"], errors="ignore").copy()

    # Ensure stable column contract/order for downstream CSV ingestion.
    for col in _EXPORT_COLUMNS:
        if col not in export_df.columns:
            export_df[col] = None
    export_df = export_df[_EXPORT_COLUMNS]

    object_name = (
        f"{params.b2_object_prefix}/DATE/{slug}-tagged-filtered-summaries.csv"
    )

    _, b2_artifact = csv_to_b2(
        export_df,
        object_name=object_name,
        add_date_slug=params.b2_add_date_slug,
        ensure_unique=params.b2_ensure_unique,
    )

    prefiltered_b2_artifact = aboutness_run.scored_rows_csv or FileUploadArtifact(
        bucket="", object_key=""
    )

    return TaggedFilteredSummariesFlowOutput(
        query_summary=query_summary,
        filter_summary=filter_summary,
        aboutness_llm_cost=aboutness_cost,
        summarizer_llm_cost=summarizer_cost,
        zeroshot_summary=zeroshot_summary,
        b2_artifact=b2_artifact,
        prefiltered_b2_artifact=prefiltered_b2_artifact,
    )

