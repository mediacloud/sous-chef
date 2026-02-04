"""
Aggregator tasks.

Tasks for aggregating and summarizing data from DataFrames.
"""
from prefect import task
import pandas as pd
from collections import Counter


@task
def top_n_unique_values(
    df: pd.DataFrame,
    column: str,
    top_n: int = 10
) -> pd.DataFrame:
    """
    Find the top-n unique values in a column from the input table.
    
    This task aggregates data by counting occurrences of each unique value
    in the specified column and returns the top-n most frequent values
    along with their counts.
    
    If the column contains lists, each unique value in a list is counted
    once per row (i.e., if a row has ['apple', 'apple', 'banana'], it counts
    as one occurrence of 'apple' and one occurrence of 'banana' for that row).
    
    Args:
        df: Input DataFrame
        column: Name of the column to find unique values in
        top_n: Number of top unique values to return
        
    Returns:
        DataFrame with two columns:
        - value: The unique values from the input column
        - count: The number of rows in which each value appears
        
        Results are sorted by count in descending order (most frequent first).
        Limited to top_n rows.
        
    Example:
        # Single values
        articles = query_online_news(...)
        # articles has: text, language, source columns
        top_languages = top_n_unique_values(articles, column="language", top_n=5)
        
        # List values
        articles = extract_keywords(articles)
        # articles now has: text, language, keywords columns
        # keywords column contains lists like ['keyword1', 'keyword2', ...]
        top_keywords = top_n_unique_values(articles, column="keywords", top_n=10)
        # Each unique keyword in a row is counted once, even if it appears
        # multiple times in that row's list
    """
    if df.empty:
        return pd.DataFrame(columns=["value", "count"])
    
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame. Available columns: {list(df.columns)}")
    
    counter = Counter()
    
    # Process each row
    for idx, row_value in df[column].items():
        # Handle None values first
        if row_value is None:
            continue
        
        # Check if the value is a list
        if isinstance(row_value, list):
            # Skip empty lists
            if len(row_value) == 0:
                continue
            # Convert to set to get unique values per row, then count each once
            unique_values = set(row_value)
            counter.update(unique_values)
        else:
            # Single value - handle NaN for scalars only (pd.isna doesn't work on lists)
            if pd.isna(row_value):
                continue
            # Single value - count once for this row
            counter[row_value] += 1
    
    # Get top_n most common values
    top_items = counter.most_common(top_n)
    
    # Convert to DataFrame
    if top_items:
        values, counts = zip(*top_items)
        return pd.DataFrame({
            "value": values,
            "count": counts
        })
    else:
        return pd.DataFrame(columns=["value", "count"])
