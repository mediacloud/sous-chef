"""
Keyword extraction tasks.

Extract keywords from texts using YAKE keyword extractor.
"""
import yake
from typing import List
from prefect import task
import pandas as pd
from .utils import add_column_from_function


def extract_keywords_row(
    text: str,
    language: str,
    top_n: int = 50,
    ngram_max: int = 3,
    dedup_limit: float = 0.9
) -> List[str]:
    """
    Extract keywords from a single text using YAKE.
    
    This is the core row-processing function.
    
    Args:
        text: Text to extract keywords from
        language: Language code (e.g., "en", "es")
        top_n: Number of top keywords to return
        ngram_max: Maximum n-gram size
        dedup_limit: Deduplication limit
        
    Returns:
        List of keywords
    """
    extractor = yake.KeywordExtractor(
        lan=language,
        n=ngram_max,
        dedupLim=dedup_limit,
        top=top_n,
        features=None
    )
    keywords = extractor.extract_keywords(text)
    # Extract just the keyword text (first element of each tuple)
    return [kw[0] for kw in keywords]


@task
def extract_keywords(
    df: pd.DataFrame,
    text_column: str = "text",
    language_column: str = "language",
    top_n: int = 50,
    ngram_max: int = 3,
    dedup_limit: float = 0.9
) -> pd.DataFrame:
    """
    Extract keywords from DataFrame texts.
    
    Adds a 'keywords' column to the DataFrame containing lists of keywords
    for each row. This keeps keywords associated with their source text.
    
    Args:
        df: DataFrame with text and language columns
        text_column: Name of column containing text
        language_column: Name of column containing language codes
        top_n: Number of top keywords to extract per text
        ngram_max: Maximum n-gram size for keyword extraction
        dedup_limit: Deduplication limit for YAKE
        
    Returns:
        DataFrame with 'keywords' column added
        
    Example:
        articles = query_online_news(...)
        # articles has: text, language columns
        
        articles = extract_keywords(articles)
        # articles now has: text, language, keywords columns
        # keywords[i] contains keywords extracted from text[i]
    """
    return add_column_from_function(
        df,
        extract_keywords_row,
        input_cols=[text_column, language_column],
        output_col="keywords",
        top_n=top_n,
        ngram_max=ngram_max,
        dedup_limit=dedup_limit
    )