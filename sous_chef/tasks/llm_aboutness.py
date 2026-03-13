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

from .llm_base import BaseLLMTask, LLMModelClient, GroqClient
from .utils import apply_llm_task_over_dataframe
from ..utils import get_logger
from ..artifacts import LLMCostSummary, ArtifactResult
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
    """

    is_about: bool = Field(
        description="True if the article is substantially about the target subject."
    )
    score: float = Field(
        ge=0.0,
        le=1.0,
        description="Confidence/degree of aboutness on a 0.0–1.0 scale.",
    )
    reason: str = Field(
        description="Short explanation of why the article is or is not about the target."
    )
    supporting_evidence: List[str] = Field(
        default_factory=list,
        description="Optional bullet points indicating evidence that the article is about the target.",
    )
    counter_evidence: List[str] = Field(
        default_factory=list,
        description="Optional bullet points indicating evidence that the article is not about the target.",
    )


_ABOUTNESS_PROMPT = """
You are an expert judge evaluating how much a news article is ABOUT a particular subject.

Your job is to decide whether the article is substantially about the subject, and to rate
your confidence / degree of aboutness on a 0.0–1.0 scale.

Subject (the thing we care about):
\"\"\"{target}\"\"\"

Article title:
\"\"\"{title}\"\"\"

Article text:
\"\"\"{text}\"\"\"

Additional background about the subject (may be empty if not provided):
\"\"\"{context}\"\"\"

Guidelines:
- The article is ABOUT the subject when the subject, its situation, actions, policies,
  impacts, or closely related events are a central focus of the story.
- Passing mentions or minor references without substantive discussion should NOT count
  as being about the subject.
- However, if the subject's government, institutions, or closely tied entities are
  clearly the focus, you should treat that as being about the subject.
- Default behavior: if there is clear, substantive discussion of the subject, treat
  it as about the subject. Only mark it as not about the subject when the connection
  is clearly incidental or absent.

Output format:
Return ONLY a JSON object with the following fields:
- "is_about": boolean, true if the article is substantially about the subject.
- "score": number between 0.0 and 1.0 indicating how strongly it is about the subject.
- "reason": short string explaining your judgment.
- "supporting_evidence": array of short strings giving evidence that it IS about the subject.
- "counter_evidence": array of short strings giving evidence that it is NOT about the subject.
""".strip()


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
            prompt_template=_ABOUTNESS_PROMPT,
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
) -> ArtifactResult[pd.DataFrame]:
    """
    Run the generic aboutness judge LLM task over each article row.

    Adds the following columns:
      - about_score: float in [0.0, 1.0]
      - about_is_about: bool
      - about_reason: short explanation string
      - about_error: error message if the LLM call failed
    """

    logger = get_logger()
    logger.info("starting aboutness LLM scoring")

    if df.empty:
        df = df.copy()
        df["about_score"] = None
        df["about_is_about"] = None
        df["about_reason"] = None
        df["about_error"] = None
        cost_summary = LLMCostSummary.from_groq_summaries(model_name, [])
        return df, cost_summary

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

    return augmented_df, cost_summary

