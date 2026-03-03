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
    model_name: GroqModelName = "qwen/qwen3-32b",
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
    if df.empty:
        df["llm_summary_struct"] = []
        df["llm_summary_text"] = []
        df["llm_summary_is_confident"] = []
        df["llm_summary_error"] = []
        return df

    logger = get_logger()
    work_df = df
    if max_rows is not None and max_rows > 0:
        work_df = df.head(max_rows).copy()

    client = GroqClient(model_name=model_name)
    task_impl = SummarizeArticleTask(client=client)

    structs: List[Optional[dict[str, Any]]] = []
    texts: List[Optional[str]] = []
    confidences: List[Optional[bool]] = []
    errors: List[Optional[str]] = []
    usage_summaries: List[Optional[Dict]] = [{}]

    for _, row in work_df.iterrows():
        title = str(row.get(title_col, "") or "")
        text = str(row.get(text_col, "") or "")
        inp = SummarizeArticleInput(title=title, text=text)
        outcome: TaskOutcome[SummarizeArticleOutput] = task_impl.run(inp)

        if outcome.ok and outcome.output is not None:
            out = outcome.output
            structs.append(out.model_dump())
            texts.append(out.summary)
            confidences.append(out.is_confident)
            errors.append(None)
            usage_summary = outcome.metadata.get("usage_summaries")
            if usage_summary:
                usage_summaries += usage_summary
        else:
            structs.append(None)
            texts.append(None)
            confidences.append(None)
            err = outcome.metadata.get("error") if outcome.metadata else None
            errors.append(err or "LLM call failed")

    groq_usage_summary = LLMCostSummary.from_groq_summaries(model_name, usage_summaries)
    logger.info(groq_usage_summary)
    work_df["llm_summary_struct"] = structs
    work_df["llm_summary_text"] = texts
    work_df["llm_summary_is_confident"] = confidences
    work_df["llm_summary_error"] = errors

    # If we worked on a subset copy, merge results back into the original frame
    if work_df is not df:
        df = df.copy()
        for col in [
            "llm_summary_struct",
            "llm_summary_text",
            "llm_summary_is_confident",
            "llm_summary_error",
        ]:
            df.loc[work_df.index, col] = work_df[col]
        return df

    return work_df, groq_usage_summary

