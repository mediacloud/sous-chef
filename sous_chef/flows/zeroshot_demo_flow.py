"""
Demo flow: MediaCloud query → zero-shot classification (BGE-M3 zeroshot) → CSV on B2.
"""
from ..flow import BaseFlowOutput, register_flow
from ..runtime import mark_step
from ..params.csv_export import CsvExportParams
from ..params.email_recipient import EmailRecipientParam
from ..params.mediacloud_query import MediacloudQuery
from ..params.webhook_callback import WebhookCallbackParam
from ..params.zeroshot import ZeroShotClassificationParams
from ..artifacts import (
    FileUploadArtifact,
    MediacloudQuerySummary,
    ZeroShotClassificationSummary,
)
from ..tasks.discovery_tasks import query_online_news
from ..tasks.export_tasks import csv_to_b2
from ..tasks.email_tasks import send_run_summary_email
from ..tasks.zeroshot_tasks import (
    DEFAULT_ZEROSHOT_MODEL,
    ZEROSHOT_CLASSIFY_DEVICE,
    ZEROSHOT_STORY_TEXT_COLUMN,
    compute_zero_shot_label_counts,
    story_dataframe_for_zeroshot_csv,
    zeroshot_classification_failure_details,
    zero_shot_classify_stories,
)
from ..utils import create_url_safe_slug, get_logger


class ZeroshotDemoParams(
    MediacloudQuery,
    ZeroShotClassificationParams,
    CsvExportParams,
    EmailRecipientParam,
    WebhookCallbackParam,
):
    """Parameters for the zero-shot classification demo flow."""


class ZeroshotDemoFlowOutput(BaseFlowOutput):
    query_summary: MediacloudQuerySummary
    zeroshot_summary: ZeroShotClassificationSummary
    b2_artifact: FileUploadArtifact


@register_flow(
    name="zeroshot_classification",
    description=(
        "Query MediaCloud, run zero-shot text classification (default: BGE-M3 zeroshot v2), "
        "and upload a per-story CSV (metadata + labels/scores, no full text) to B2."
    ),
    params_model=ZeroshotDemoParams,
    output_model=ZeroshotDemoFlowOutput,
    log_prints=True,
)
def zeroshot_demo_flow(params: ZeroshotDemoParams) -> ZeroshotDemoFlowOutput:
    logger = get_logger()
    logger.info("starting zeroshot_classification flow")

    articles, query_summary = query_online_news(
        query=params.query,
        collection_ids=params.collection_ids,
        source_ids=params.source_ids,
        start_date=params.start_date,
        end_date=params.end_date,
        dedup_strategy=params.dedup_strategy,
        upload_dedup_summary=params.upload_dedup_summary,
    )

    if params.max_stories is not None:
        articles = articles.head(params.max_stories).copy()

    mark_step(
        "zeroshot_classification_start",
        meta={
            "stories": len(articles),
            "labels": len(params.classification_labels),
            "multi_label": params.multi_label,
        },
    )
    articles = zero_shot_classify_stories(
        articles,
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
            articles,
            params.classification_labels,
            params.zeroshot_score_threshold,
        )
    )
    failure_details = zeroshot_classification_failure_details(articles)
    mark_step(
        "zeroshot_classification_end",
        meta={
            "stories": len(articles),
            "classification_failures": stories_failed,
        },
    )

    zeroshot_summary = ZeroShotClassificationSummary(
        input_labels=params.classification_labels,
        label_counts=label_counts,
        stories_classified=len(articles),
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

    export_df = story_dataframe_for_zeroshot_csv(articles)

    slug = create_url_safe_slug(params.query)
    object_name = f"{params.b2_object_prefix}/DATE/{slug}-zeroshot-stories.csv"
    logger.info("uploading zeroshot story CSV: %s", object_name)
    _, b2_artifact = csv_to_b2(
        export_df,
        object_name=object_name,
        add_date_slug=params.b2_add_date_slug,
        ensure_unique=params.b2_ensure_unique,
    )

    if params.email_to:
        send_run_summary_email(
            email_to=params.email_to,
            query_summary=query_summary,
            b2_artifact=b2_artifact,
            flow_name="zeroshot_classification",
            query=params.query,
        )

    return ZeroshotDemoFlowOutput(
        query_summary=query_summary,
        zeroshot_summary=zeroshot_summary,
        b2_artifact=b2_artifact,
    )
