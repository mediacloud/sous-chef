from __future__ import annotations

"""
Demo structured LLM task: summarize news articles with confidence metadata.

Provides:
- SummarizeArticleInput / SummarizeArticleOutput Pydantic models
- SummarizeArticleTask: BaseLLMTask implementation
- summarize_articles_llm: Prefect task that applies the LLM task to a DataFrame
"""

from typing import Any, List, Optional, Dict

import pandas as pd
from prefect import task
from pydantic import BaseModel, Field

from .llm_base import BaseLLMTask, LLMModelClient, GroqClient, TaskOutcome
from .utils import apply_llm_task_over_dataframe
from ..utils import get_logger
from ..artifacts import LLMCostSummary, ArtifactResult
from ..params import GroqModelName

class SummarizeArticleInput(BaseModel):
    title: str
    text: str
    language: Optional[str] = None


class SummarizeArticleOutput(BaseModel):
    summary: Optional[str]
    is_confident: bool
    confidence_reason: str
    key_points: List[str] = Field(default_factory=list)


_SUMMARY_PROMPT = """
You are an expert news analyst. Read the following article and produce a concise summary and key bullet points.

Return ONLY a JSON object with the following fields:
- "summary": string or null
- "is_confident": boolean
- "confidence_reason": short string explaining why you are or are not confident
- "key_points": array of short strings

If you cannot confidently summarize (for example the text is too short, not in a language you understand, or off-topic),
set "summary" to null, "is_confident" to false, and explain why in "confidence_reason".

Article title: "{title}"

Article text:
\"\"\"{text}\"\"\"
""".strip()


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
            prompt_template=_SUMMARY_PROMPT,
        )


@task
def summarize_articles_llm(
    df: pd.DataFrame,
    text_col: str = "text",
    title_col: str = "title",
    model_name: GroqModelName = GroqModelName.llama,
    max_rows: Optional[int] = None,
) -> ArtifactResult[pd.DataFrame]:
    """
    Run a structured LLM summarization task over each article row.

    Adds the following columns:
      - llm_summary_struct: dict representation of SummarizeArticleOutput or None
      - llm_summary_text: summary text or None
      - llm_summary_is_confident: bool or None
      - llm_summary_error: error message if the LLM call failed
    """
    logger = get_logger()

    #Initialize new columns
    df["llm_summary_text"] = None
    df["llm_summary_is_confident"] = None
    df["llm_summary_error"] = None

    #Bailout if empty
    if df.empty:
        cost_summary = LLMCostSummary.from_groq_summaries(model_name, [])
        return df, cost_summary

    #Work on a copy of the dataframe-easiest way to limit the number of rows.
    work_df = df
    if max_rows is not None and max_rows > 0:
        work_df = df.head(max_rows).copy()

    
    client = GroqClient(model_name=model_name)
    task_impl = SummarizeArticleTask(client=client)

    #Some local mapping callbacks, map the input and output columns to the task structure
    def build_input(row: Dict[str, Any]) -> SummarizeArticleInput:
        title = str(row.get(title_col, "") or "")
        text = str(row.get(text_col, "") or "")
        return SummarizeArticleInput(title=title, text=text)

    def map_output_to_row(output: SummarizeArticleOutput) -> Dict[str, Any]:
        return {
            "llm_summary_text": output.summary,
            "llm_summary_is_confident": output.is_confident,
        }

    # Apply LLM task over the DataFrame
    augmented_df, usages, outcomes = apply_llm_task_over_dataframe(
        work_df.copy(),
        task_impl,
        build_input,
        map_output_to_row,
        error_col="llm_summary_error"
    )

    # Aggregate usage into LLMCostSummary artifact
    groq_usage_summary = LLMCostSummary.from_groq_summaries(model_name, usages)

    #Merge results back into the original dataframe
    if augmented_df is not df:
        df = df.copy()
        for col in [
            "llm_summary_text",
            "llm_summary_is_confident",
            "llm_summary_error",
        ]:
            df.loc[augmented_df.index, col] = augmented_df[col]
        return df, groq_usage_summary

    return augmented_df, groq_usage_summary

