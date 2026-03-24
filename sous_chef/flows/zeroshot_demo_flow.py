"""
Demo flow: MediaCloud query → zero-shot classification (BGE-M3 zeroshot) → CSV on B2.
"""
from typing import List, Optional

from pydantic import Field, field_validator

from ..flow import BaseFlowOutput, register_flow
from ..params.csv_export import CsvExportParams
from ..params.email_recipient import EmailRecipientParam
from ..params.mediacloud_query import MediacloudQuery
from ..params.webhook_callback import WebhookCallbackParam
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
    compute_zero_shot_label_counts,
    story_dataframe_for_zeroshot_csv,
    zero_shot_classify_stories,
)
from ..utils import create_url_safe_slug, get_logger


class ZeroshotDemoParams(
    MediacloudQuery,
    CsvExportParams,
    EmailRecipientParam,
    WebhookCallbackParam,
):
    """Parameters for the zero-shot classification demo flow."""

    classification_labels: List[str] = Field(
        default_factory=lambda: [
            "politics",
            "economy",
            "environment",
            "technology",
            "health",
        ],
        title="Classification labels",
        description='Verbalized class names passed to the zero-shot model (e.g. "politics", "sports").',
    )
    hypothesis_template: str = Field(
        default="This text is about {}",
        title="Hypothesis template",
        description='NLI hypothesis with "{}" where the label is inserted (Hugging Face zero-shot format).',
    )
    multi_label: bool = Field(
        default=True,
        title="Multi-label",
        description="If true, scores all labels; if false, the pipeline picks a single best label.",
    )
    zeroshot_model: str = Field(
        default=DEFAULT_ZEROSHOT_MODEL,
        title="Hugging Face model id",
        description="Zero-shot classification model (default: MoritzLaurer/bge-m3-zeroshot-v2.0).",
    )
    text_column: str = Field(
        default="text",
        title="Text column",
        description="DataFrame column to classify.",
    )
    text_max_chars: Optional[int] = Field(
        default=12000,
        title="Max characters per story",
        description="Truncate text before inference (speed/memory). Null = no truncation.",
    )
    max_stories: Optional[int] = Field(
        default=None,
        ge=1,
        title="Max stories to classify",
        description="Optional cap after querying MediaCloud (recommended for local CPU demos).",
    )
    classify_device: int = Field(
        default=-1,
        title="Torch device",
        description="-1 for CPU, or a non-negative CUDA device index if a GPU is available.",
    )
    zeroshot_score_threshold: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        title="Score threshold (optional)",
        description=(
            "If set: summary distribution counts a label per story when its score ≥ this value; "
            "adds zeroshot_labels_passing_threshold_json to the export. "
            "If unset: distribution uses only the single top label per story."
        ),
    )
    csv_story_columns: Optional[List[str]] = Field(
        default=None,
        title="CSV story columns",
        description=(
            "Which columns to include in the uploaded CSV (excluding full text). "
            "If unset, uses default metadata columns plus all zeroshot_* columns."
        ),
    )

    @field_validator("hypothesis_template")
    @classmethod
    def _must_contain_brace_placeholder(cls, v: str) -> str:
        if "{}" not in v:
            raise ValueError('hypothesis_template must contain "{}" for the class label')
        return v

    @field_validator("classification_labels")
    @classmethod
    def _non_empty_labels(cls, v: List[str]) -> List[str]:
        stripped = [s.strip() for s in v if s and str(s).strip()]
        if not stripped:
            raise ValueError("classification_labels must contain at least one non-empty label")
        return stripped


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

    articles = zero_shot_classify_stories(
        articles,
        params.classification_labels,
        text_column=params.text_column,
        hypothesis_template=params.hypothesis_template,
        multi_label=params.multi_label,
        model=params.zeroshot_model,
        device=params.classify_device,
        text_max_chars=params.text_max_chars,
        passing_score_threshold=params.zeroshot_score_threshold,
    )

    label_counts, stories_without_prediction = compute_zero_shot_label_counts(
        articles,
        params.classification_labels,
        params.zeroshot_score_threshold,
    )

    zeroshot_summary = ZeroShotClassificationSummary(
        input_labels=params.classification_labels,
        label_counts=label_counts,
        stories_classified=len(articles),
        stories_without_prediction=stories_without_prediction,
        summary_score_threshold=params.zeroshot_score_threshold,
        distribution_mode=(
            "threshold_ge"
            if params.zeroshot_score_threshold is not None
            else "top_label"
        ),
        multi_label=params.multi_label,
        hypothesis_template=params.hypothesis_template,
        model_id=params.zeroshot_model,
    )

    export_df = story_dataframe_for_zeroshot_csv(articles, params.csv_story_columns)

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
