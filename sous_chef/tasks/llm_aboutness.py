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


_ABOUTNESS_PROMPT = """
You are an expert judge evaluating how much a news article is ABOUT a particular subject.

Assume the article was selected because it already matches a query related to this subject.
Your primary job is to catch cases where the subject is only mentioned incidentally or
clearly refers to something else, and to rate your confidence / degree of aboutness on
a 0.0–1.0 scale.

Subject (the thing we care about):
\"\"\"{target}\"\"\"

Article title:
\"\"\"{title}\"\"\"

Article text:
\"\"\"{text}\"\"\"

Additional background about the subject (may be empty if not provided):
\"\"\"{context}\"\"\"

Guidelines (negative filter):
- By default, assume the article is about the subject, because it was selected by a
  query that already searched for this subject.
- Mark the article as NOT about the subject only when there is clear evidence that:
  * The subject is mentioned only briefly or incidentally (for example, in a long list
    of places, organizations, people, or in a single short dateline or quote), AND
    there is no meaningful discussion of the subject’s situation, actions, decisions,
    impacts, or related events; OR
  * The subject clearly refers to something else entirely (for example, a different
    person, company, product, or place that happens to share the same name), and not
    to the intended subject; OR
  * The subject appears only in boilerplate or background text with no connection to
    the main story.
- If the subject appears multiple times, is involved in the main events being
  described, or there is any sustained discussion related to the subject, you should
  treat the article as being ABOUT the subject.
- When you are unsure, or when there is some sustained discussion of the subject,
  lean toward treating the article as being ABOUT the subject rather than rejecting it.

Scoring:
- Let "score" be how strongly you believe the article SHOULD be kept as being about the
  subject (higher score = more clearly about the subject).

Output format:
Return ONLY a JSON object with the following fields:
- "is_about": boolean, true if the article should be treated as about the subject.
- "score": number between 0.0 and 1.0 indicating how strongly it should be kept as
  about the subject.
- "reason": short string explaining your judgment. In this field, briefly mention both
  any evidence that it IS about the subject and any evidence that it is NOT, keeping
  the explanation to 1–3 short sentences.
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

