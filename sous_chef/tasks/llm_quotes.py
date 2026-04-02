from __future__ import annotations

"""
"""

from typing import Any, List, Optional, Dict
from enum import Enum


import pandas as pd
from prefect import task
from pydantic import BaseModel

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



_QUOTE_EXTRACTION_PROMPT = """

You are a quote extraction system. Your task is to analyze a news story and extract all direct quotations along with speaker attribution and pronouns.

TASK REQUIREMENTS:
1. Extract ONLY direct quotations that appear in the story text
2. Identify the speaker for each quote (full name if possible)
3. Identify the pronoun used in the text to refer to the speaker
4. Return ONLY valid JSON with no other explanation or text

EXTRACTION RULES:
- Extract quotes exactly as they appear in the text, without surrounding quotation marks.
- Be accurate. Do not embellish of add any text to what is included in the quotation marks in the story.
- If a quote uses a pronoun or partial name (e.g., "he said") instead of a full name, resolve it to the complete name mentioned earlier in the story
- If the speaker cannot be identified, use "unspecified"
- If no pronoun is used in the text to refer to the speaker, use "unspecified"
- Valid pronoun values: "he", "she", "they", or "unspecified"
- Do not guess the pronoun from the name. Only include pronouns actually resolved to identified speakers of the quote.
- If there are NO quotes in the story, return {"quotes": []}

OUTPUT JSON SCHEMA:
{
  "quotes": [
    {
      "quote": "the exact quote text without surrounding quote marks",
      "speaker": "full name of speaker or 'unspecified'",
      "pronoun": "he OR she OR they OR unspecified"
    }
  ]
}

EXAMPLE:
Input: "Mayor Johnson spoke at the event. She said, 'We must invest in infrastructure.' Johnson also noted, 'Education is key.' He meant this seriously."

Output:
{
  "quotes": [
    {
      "quote": "We must invest in infrastructure",
      "speaker": "Mayor Johnson",
      "pronoun": "she"
    },
    {
      "quote": "Education is key",
      "speaker": "Mayor Johnson",
      "pronoun": "he"
    }
  ]
}

OUTPUT: Return ONLY the JSON object. Do not include any explanation, markdown formatting, or additional text.

Here is the article to analyze:
{text}
""".strip()


class ArticleQuotesTask(
    BaseLLMTask[ArticleQuotesInput, ArticleQuotesOutput]
):
    """
    Structured LLM task that summarizes a single article.
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
            prompt_template=_QUOTE_EXTRACTION_PROMPT,
        )


@task
def article_quotes_llm(
    df: pd.DataFrame,
    text_col: str = "text",
    model_name: GroqModelName = GroqModelName.llama_versatile,
    max_rows: Optional[int] = None,
) -> ArtifactResult[pd.DataFrame]:
    """
    Run a structured LLM summarization task over each article row.
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
                "pronoun": str(quote.pronoun),
            }
            for quote in output.quotes
        ]

    # Apply LLM task over the DataFrame
    results_df, usages, outcomes = apply_llm_task_over_dataframe(
        df.copy(),
        task_impl,
        build_input,
        map_output_to_row,
        error_col="llm_summary_error"
    )


    # Aggregate usage into LLMCostSummary artifact - again hardcoded to groq for now
    groq_usage_summary = LLMCostSummary.from_groq_summaries(model_name, usages)


    return results_df, groq_usage_summary
