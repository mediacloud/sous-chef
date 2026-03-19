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
You are a robot that processes news stories that I send you and produces JSON content containing all the quotes in 
the text, who said them, and what pronoun is used to refer to the speaker. I will input text and you should process 
it to extract all the quotes, speakers, and their pronouns in the text I send you. Only include quotes that are included 
in the story, do not include any other text. If there are no quotes, return a result with an empty quotes array. 
If the quote uses a pronoun or partial name to indicate who said it then you should try to resolve that to the full 
name of the person who said it, which might be mentioned earlier in the story. I also want to know what pronoun is 
used in the text to refer to each speaker - "he", "she", or "they".  

Give me back the JSON in text format and no other explanation. The resulting JSON you create should have a 
top-level "quotes" property that is an array of objects. Each object should have three properties: 
- the "quote" property should be the quote that was in the text without surrounding quote marks
- the "speaker" property should be the full name of the person that said the quote
- and the "pronoun" property should be the pronoun that is used in the text to identify the speaker

If there is no person that can be identified as the speaker, set the "speaker" property to "unspecified", otherwise
try to include the person's whole name. If there isn't a pronoun used to identify the speaker of the quote anywhere 
then set the "pronoun" property to "unspecified". The value of the "pronoun" property should be "he", "she", "they", 
or "unspecified". 

Here is the article text that I want you to analyze:
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
    model_name: GroqModelName = GroqModelName.llama,
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
