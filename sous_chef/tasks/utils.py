"""
Utilities for working with DataFrames in tasks.

Common patterns for applying functions to DataFrame rows and adding results as columns.
"""
from typing import Callable, List
import pandas as pd


def add_column_from_function(
    df: pd.DataFrame,
    func: Callable,
    input_cols: List[str],
    output_col: str,
    **func_kwargs
) -> pd.DataFrame:
    """
    Apply function to each row, add result as new column.
    
    This is a common pattern: take a DataFrame, apply a function per-row using
    values from specified columns, and add the result as a new column alongside
    the original data. This keeps related data together (e.g., keywords with
    their source text).
    
    Args:
        df: Input DataFrame
        func: Function(input_col_values...) -> output_value
        input_cols: Column names to extract from each row and pass as args to func
        output_col: Name of new column to add
        **func_kwargs: Additional keyword arguments to pass to func
        
    Returns:
        DataFrame with new column added
        
    Example:
        def extract_keywords(text: str, language: str, top_n: int = 50) -> List[str]:
            # Extract keywords from text
            return keywords_list
        
        df = add_column_from_function(
            articles_df,
            extract_keywords,
            input_cols=["text", "language"],
            output_col="keywords",
            top_n=100
        )
        # Result: DataFrame with "keywords" column added, keeping text and keywords together
    """
    if df.empty:
        df[output_col] = []
        return df
    
    def row_wrapper(row):
        args = [row[col] for col in input_cols]
        return func(*args, **func_kwargs)
    
    df[output_col] = df.apply(row_wrapper, axis=1)
    return df


def add_columns_from_function(
    df: pd.DataFrame,
    func: Callable,
    input_cols: List[str],
    output_cols: List[str],
    **func_kwargs
) -> pd.DataFrame:
    """
    Apply function to each row, add results as multiple columns.
    
    Function should return dict with keys matching output_cols.
    
    Args:
        df: Input DataFrame
        func: Function(input_col_values...) -> {col1: val1, col2: val2}
        input_cols: Column names to extract from each row and pass as args to func
        output_cols: Names of new columns to add
        **func_kwargs: Additional keyword arguments to pass to func
        
    Returns:
        DataFrame with new columns added
        
    Example:
        def extract_entities(text: str, language: str) -> dict:
            # Extract entities
            return {
                "entities": entities_list,
                "entity_count": len(entities_list)
            }
        
        df = add_columns_from_function(
            articles_df,
            extract_entities,
            input_cols=["text", "language"],
            output_cols=["entities", "entity_count"]
        )
        # Result: DataFrame with both "entities" and "entity_count" columns added
    """
    if df.empty:
        for col in output_cols:
            df[col] = []
        return df
    
    def row_wrapper(row):
        args = [row[col] for col in input_cols]
        result = func(*args, **func_kwargs)
        if not isinstance(result, dict):
            raise ValueError(
                f"Function must return dict with keys {output_cols}, "
                f"got {type(result).__name__}"
            )
        return result
    
    results = df.apply(row_wrapper, axis=1)
    result_df = pd.DataFrame(results.tolist())
    
    for col in output_cols:
        if col not in result_df.columns:
            raise ValueError(
                f"Function did not return column '{col}'. "
                f"Got: {list(result_df.columns)}"
            )
        df[col] = result_df[col]
    
    return df
