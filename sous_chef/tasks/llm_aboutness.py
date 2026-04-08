from __future__ import annotations

"""
Generic LLM \"aboutness\" judge task.

This module provides:
- AboutnessInput / AboutnessOutput Pydantic models
- AboutnessTask: BaseLLMTask implementation
- score_aboutness_llm: Prefect task that applies the LLM task to a DataFrame
"""

from typing import Any, Dict, List, Optional

import pandas as pd
from prefect import task
from pydantic import BaseModel, Field, field_validator

from .export_tasks import csv_to_b2
from .llm_base import BaseLLMTask, LLMModelClient, GroqClient
from .utils import apply_llm_task_over_dataframe
from ..prompts import load_prompt
from ..utils import get_logger
from ..artifacts import (
    LLMCostSummary,
    ArtifactResult,
    AboutnessScoringRunArtifact,
    FileUploadArtifact,
)
from ..params import GroqModelName


class AboutnessInput(BaseModel):
    """
    Input to the aboutness judge task.

    This is intentionally generic: it can be used for states, topics,
    organizations, people, etc.
    """

    title: str
    text: str
    target: str = Field(
        description="The subject we are judging aboutness against (e.g., a state, topic, organization, or person)."
    )
    context: Optional[str] = Field(
        default=None,
        description="Optional background about the target to help the model judge aboutness (e.g., LGA list, topic description).",
    )

    @field_validator("title", "text", "target", mode="before")
    @classmethod
    def coerce_to_str(cls, v: Any) -> str:
        # Be defensive about None / non-strings coming from DataFrames
        return "" if v is None else str(v)


class AboutnessOutput(BaseModel):
    """
    Structured output from the aboutness judge.

    Kept intentionally simple for robustness: a boolean decision, a numeric
    score, and a short free-text explanation.
    """

    is_about: bool = Field(
        description="True if the article should be treated as being about the target subject."
    )
    score: float = Field(
        ge=0.0,
        le=1.0,
        description="How strongly this article should be kept as about the subject, on a 0.0–1.0 scale.",
    )
    reason: str = Field(
        description=(
            "Short explanation of your judgment, ideally mentioning both why it might "
            "be about the subject and why it might not. Keep this to 1–3 short sentences."
        )
    )


class AboutnessTask(BaseLLMTask[AboutnessInput, AboutnessOutput]):
    """
    Structured LLM task that judges whether an article is about a target subject.
    """

    input_model = AboutnessInput
    output_model = AboutnessOutput

    def __init__(self, client: LLMModelClient) -> None:
        super().__init__(
            client=client,
            task_name="aboutness_judge",
            description=(
                "Judge whether a news article is substantially about a target subject "
                "and return a confidence/degree-of-aboutness score."
            ),
            prompt_template=load_prompt("aboutness", "v1.txt"),
        )


@task
def score_aboutness_llm(
    df: pd.DataFrame,
    target: str,
    context: Optional[str] = None,
    text_col: str = "text",
    title_col: str = "title",
    model_name: GroqModelName = GroqModelName.llama,
    max_rows: Optional[int] = None,
    upload_scored_rows_csv: bool = False,
    scored_csv_object_name: Optional[str] = None,
    b2_add_date_slug: bool = True,
    b2_ensure_unique: bool = True,
    drop_text_for_scored_csv: bool = True,
) -> ArtifactResult[pd.DataFrame]:
    """
    Run the generic aboutness judge LLM task over each article row.

    Adds the following columns:
      - about_score: float in [0.0, 1.0]
      - about_is_about: bool
      - about_reason: short explanation string
      - about_error: error message if the LLM call failed

    When ``upload_scored_rows_csv`` is True and ``scored_csv_object_name`` is
    set, uploads all scored rows to B2 (optional full-text strip via
    ``drop_text_for_scored_csv``), similar to optional dedup CSV upload in
    ``query_online_news``.

    Returns:
        Tuple of ``(scored DataFrame, AboutnessScoringRunArtifact)``.
    """

    logger = get_logger()
    logger.info("starting aboutness LLM scoring")

    def _run_artifact(cost: LLMCostSummary, scored_csv: Optional[FileUploadArtifact]) -> AboutnessScoringRunArtifact:
        return AboutnessScoringRunArtifact(llm_cost=cost, scored_rows_csv=scored_csv)

    if df.empty:
        df = df.copy()
        df["about_score"] = None
        df["about_is_about"] = None
        df["about_reason"] = None
        df["about_error"] = None
        cost_summary = LLMCostSummary.from_groq_summaries(model_name, [])
        scored_csv = None
        if upload_scored_rows_csv and scored_csv_object_name:
            export_df = df.copy()
            if drop_text_for_scored_csv:
                export_df = export_df.drop(columns=["text"], errors="ignore")
            _, scored_csv = csv_to_b2(
                export_df,
                object_name=scored_csv_object_name,
                add_date_slug=b2_add_date_slug,
                ensure_unique=b2_ensure_unique,
            )
        elif upload_scored_rows_csv and not scored_csv_object_name:
            logger.warning(
                "upload_scored_rows_csv is True but scored_csv_object_name is missing; skipping B2 upload"
            )
        return df, _run_artifact(cost_summary, scored_csv)

    # Truncate for safety/cost control if requested
    if max_rows is not None and max_rows > 0:
        df = df.head(max_rows).copy()

    client = GroqClient(model_name=model_name)
    task_impl = AboutnessTask(client=client)

    def build_input(row: Dict[str, Any]) -> AboutnessInput:
        title = row.get(title_col, "") or ""
        text = row.get(text_col, "") or ""
        return AboutnessInput(
            title=title,
            text=text,
            target=target,
            context=context,
        )

    def map_output_to_row(output: AboutnessOutput) -> Dict[str, Any]:
        return {
            "about_score": output.score,
            "about_is_about": output.is_about,
            "about_reason": output.reason,
        }

    augmented_df, usages, outcomes = apply_llm_task_over_dataframe(
        df.copy(),
        task_impl,
        build_input,
        map_output_to_row,
        error_col="about_error",
    )

    cost_summary = LLMCostSummary.from_groq_summaries(model_name, usages)

    scored_csv = None
    if upload_scored_rows_csv and scored_csv_object_name:
        export_df = augmented_df.copy()
        if drop_text_for_scored_csv:
            export_df = export_df.drop(columns=["text"], errors="ignore")
        _, scored_csv = csv_to_b2(
            export_df,
            object_name=scored_csv_object_name,
            add_date_slug=b2_add_date_slug,
            ensure_unique=b2_ensure_unique,
        )
    elif upload_scored_rows_csv and not scored_csv_object_name:
        logger.warning(
            "upload_scored_rows_csv is True but scored_csv_object_name is missing; skipping B2 upload"
        )

    return augmented_df, _run_artifact(cost_summary, scored_csv)

