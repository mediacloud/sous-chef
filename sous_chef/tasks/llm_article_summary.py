from __future__ import annotations

"""
Demo structured LLM task: summarize news articles with confidence metadata.

Provides:
- SummarizeArticleInput / SummarizeArticleOutput Pydantic models
- SummarizeArticleTask: BaseLLMTask implementation
- summarize_articles_llm: Prefect task that applies the LLM task to a DataFrame
"""

import json
from typing import Any, List, Optional, Dict

import pandas as pd
from prefect import task
from pydantic import BaseModel, Field

from ..prompts import load_prompt
from .llm_base import BaseLLMTask, LLMModelClient, GroqClient
from .zeroshot.config import ZEROSHOT_UNKNOWN_LABEL
from .utils import apply_llm_task_over_dataframe
from ..utils import get_logger
from ..artifacts import LLMCostSummary, ArtifactResult
from ..params import GroqModelName


def format_zeroshot_tags_for_summary_row(
    row: Dict[str, Any],
    *,
    max_labels: int = 5,
) -> Optional[str]:
    """
    Build a short string of predicted zeroshot labels for summarizer steering.

    Uses selected labels when present, else the top label.
    """
    err = row.get("zeroshot_error")
    if err is not None and str(err).strip():
        return None

    raw_selected = row.get("zeroshot_labels_selected_json")
    if raw_selected is not None and str(raw_selected).strip():
        try:
            labs = json.loads(raw_selected)
            if isinstance(labs, list) and labs:
                parts = [
                    str(x).strip()
                    for x in labs[:max_labels]
                    if str(x).strip() and str(x).strip() != ZEROSHOT_UNKNOWN_LABEL
                ]
                if parts:
                    return "; ".join(parts)
        except (json.JSONDecodeError, TypeError):
            pass

    top = row.get("zeroshot_top_label")
    if (
        isinstance(top, str)
        and top.strip()
        and top.strip() != ZEROSHOT_UNKNOWN_LABEL
    ):
        return top.strip()
    return None


class SummarizeArticleInput(BaseModel):
    title: str
    text: str
    language: Optional[str] = None
    focus_context: Optional[str] = Field(
        default=None,
        description=(
            "Optional run-level themes or audience focus (e.g. project intent, query framing)."
        ),
    )
    predicted_focus_labels: Optional[str] = Field(
        default=None,
        description=(
            "Optional per-article tags from an automated classifier; may be wrong or incomplete."
        ),
    )


class SummarizeArticleOutput(BaseModel):
    summary: Optional[str]
    is_confident: bool
    confidence_reason: str
    key_points: List[str] = Field(default_factory=list)


class SummarizeArticleTask(
    BaseLLMTask[SummarizeArticleInput, SummarizeArticleOutput]
):
    """
    Structured LLM task that summarizes a single article.
    """

    input_model = SummarizeArticleInput
    output_model = SummarizeArticleOutput

    def __init__(self, client: LLMModelClient) -> None:
        super().__init__(
            client=client,
            task_name="summarize_article",
            description=(
                "Summarize a news article with confidence metadata and key points."
            ),
            prompt_template=load_prompt("article_summary", "v1.txt"),
        )

    def build_prompt(self, data: SummarizeArticleInput) -> str:
        payload = data.model_dump()
        for key in ("focus_context", "predicted_focus_labels"):
            if payload.get(key) is None:
                payload[key] = ""
        return self.prompt_template.format(**payload)


@task
def summarize_articles_llm(
    df: pd.DataFrame,
    text_col: str = "text",
    title_col: str = "title",
    model_name: GroqModelName = GroqModelName.llama,
    max_rows: Optional[int] = None,
    focus_context: Optional[str] = None,
    use_zeroshot_row_tags: bool = False,
    max_zeroshot_tags_for_summary: int = 5,
) -> ArtifactResult[pd.DataFrame]:
    """
    Run a structured LLM summarization task over each article row.

    Adds the following columns:
      - llm_summary_struct: dict representation of SummarizeArticleOutput or None
      - llm_summary_text: summary text or None
      - llm_summary_is_confident: bool or None
      - llm_summary_error: error message if the LLM call failed

    Optional steering:
      - ``focus_context``: same run-level string passed into every row's prompt.
      - ``use_zeroshot_row_tags``: if True, reads zeroshot columns on each row
        (``zeroshot_labels_selected_json`` or ``zeroshot_top_label``)
        and passes them as ``predicted_focus_labels``.
    """
    logger = get_logger()

    if df.empty:
        df = df.copy()
        df["llm_summary_text"] = None
        df["llm_summary_is_confident"] = None
        df["llm_summary_error"] = None
        cost_summary = LLMCostSummary.from_groq_summaries(model_name, [])
        return df, cost_summary

    # Truncate if max_rows is set (this becomes our working DataFrame)
    if max_rows is not None and max_rows > 0:
        df = df.head(max_rows).copy()
    
    client = GroqClient(model_name=model_name) #Hardcoded for now
    task_impl = SummarizeArticleTask(client=client)

    #Some local mapping callbacks, map the input and output columns to the task structure
    def build_input(row: Dict[str, Any]) -> SummarizeArticleInput:
        title = str(row.get(title_col, "") or "")
        text = str(row.get(text_col, "") or "")
        predicted: Optional[str] = None
        if use_zeroshot_row_tags:
            predicted = format_zeroshot_tags_for_summary_row(
                row,
                max_labels=max_zeroshot_tags_for_summary,
            )
        return SummarizeArticleInput(
            title=title,
            text=text,
            focus_context=focus_context,
            predicted_focus_labels=predicted,
        )

    def map_output_to_row(output: SummarizeArticleOutput) -> Dict[str, Any]:
        return {
            "llm_summary_text": output.summary,
            "llm_summary_is_confident": output.is_confident,
        }

    # Apply LLM task over the DataFrame
    augmented_df, usages, outcomes = apply_llm_task_over_dataframe(
        df.copy(),
        task_impl,
        build_input,
        map_output_to_row,
        error_col="llm_summary_error"
    )

    # Aggregate usage into LLMCostSummary artifact - again hardcoded to groq for now
    groq_usage_summary = LLMCostSummary.from_groq_summaries(model_name, usages)


    return augmented_df, groq_usage_summary

