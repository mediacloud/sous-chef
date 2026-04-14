from __future__ import annotations

"""
"""

from typing import Any, List, Optional, Dict
from enum import Enum


import pandas as pd
from prefect import task
from pydantic import BaseModel

from ..prompts import load_prompt
from .llm_base import BaseLLMTask, LLMModelClient, GroqClient
from .utils import apply_llm_task_over_dataframe
from ..artifacts import LLMCostSummary, ArtifactResult
from ..params import GroqModelName

class ArticleQuotesInput(BaseModel):
    text: str

class ValidPronouns(str, Enum):  # closely coupled with prompt
    unspecified = 'unspecified'
    he = 'he'
    she = 'she'
    they = 'they'

class ArticleQuote(BaseModel):  # closely coupled with prompt
    quote: str
    speaker: str
    pronoun: ValidPronouns

class ArticleQuotesOutput(BaseModel):
    quotes: List[ArticleQuote]

    @staticmethod
    def from_llm_output(llm_output: List[Dict[str, str]]) -> ArticleQuotesOutput:
        found_quotes = []
        for o in llm_output:
            q = ArticleQuotesOutput()
            q.quote = o["quote"]
            q.speaker = o["speaker"]
            q.pronoun = o['pronoun']
            found_quotes.append(q)
        shaped_output = ArticleQuotesOutput()
        shaped_output.quotes = found_quotes
        return shaped_output


class ArticleQuotesTask(
    BaseLLMTask[ArticleQuotesInput, ArticleQuotesOutput]
):
    """
    Structured LLM task that extracts direct quotes from a single article.
    """

    input_model = ArticleQuotesInput
    output_model = ArticleQuotesOutput

    def __init__(self, client: LLMModelClient) -> None:
        super().__init__(
            client=client,
            task_name="article_quotes",
            description=(
                "Extract quotes from an article and speaker info."
            ),
            prompt_template=load_prompt("quotes", "v1.txt"),
        )

    def build_prompt(self, data: ArticleQuotesInput) -> str:
        # Prompt includes literal JSON braces, so use targeted replacement
        # instead of str.format() to avoid treating JSON keys as placeholders.
        return self.prompt_template.replace("{text}", data.text)


@task
def article_quotes_llm(
    df: pd.DataFrame,
    text_col: str = "text",
    model_name: GroqModelName = GroqModelName.llama_versatile,
    max_rows: Optional[int] = None,
) -> ArtifactResult[pd.DataFrame]:
    """
    Extract quotes per article using the structured LLM task.
    Output is **one row per quote** (list-shaped mapping in
    ``apply_llm_task_over_dataframe``). Non-quote columns from the input
    ``df`` are copied onto each quote row so you can group or join back to the
    parent story on whatever id columns the caller included. The full-text
    ``text_col`` is dropped from the returned frame after extraction.
    LLM failures omit that article from ``results_df`` entirely; inspect the
    third return value from ``apply_llm_task_over_dataframe`` (``outcomes``) if
    you need per-article success or error detail.
    """

    if df.empty:
        df = df.copy()
        cost_summary = LLMCostSummary.from_groq_summaries(model_name, [])
        return df, cost_summary

    # Truncate if max_rows is set (this becomes our working DataFrame)
    if max_rows is not None and max_rows > 0:
        df = df.head(max_rows).copy()
    
    client = GroqClient(model_name=model_name) #Hardcoded for now
    task_impl = ArticleQuotesTask(client=client)

    #Some local mapping callbacks, map the input and output columns to the task structure
    def build_input(row: Dict[str, Any]) -> ArticleQuotesInput:
        text = str(row.get(text_col, "") or "")
        return ArticleQuotesInput(text=text)

    def map_output_to_row(output: ArticleQuotesOutput) -> List[Dict[str, Any]]:
        return [
            {
                "quote": quote.quote,
                "speaker": quote.speaker,
                "pronoun": quote.pronoun.value,
            }
            for quote in output.quotes
        ]

    # Apply LLM task over the DataFrame
    results_df, usages, outcomes = apply_llm_task_over_dataframe(
        df.copy(),
        task_impl,
        build_input,
        map_output_to_row,
        error_col="llm_error"
    )
    results_df.drop(columns=[text_col], inplace=True)

    # Aggregate usage into LLMCostSummary artifact - again hardcoded to groq for now
    groq_usage_summary = LLMCostSummary.from_groq_summaries(model_name, usages)


    return results_df, groq_usage_summary
