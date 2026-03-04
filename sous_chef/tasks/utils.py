"""
Utilities for working with DataFrames in tasks.

Common patterns for applying functions to DataFrame rows and adding results as columns,
plus helpers for running LLM tasks over DataFrames.
"""
from typing import Callable, List, Tuple, Any, Dict
from pydantic import BaseModel  
import pandas as pd

from .llm_base import BaseLLMTask, TaskOutcome, run_llm_task_over_rows


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


def apply_llm_task_over_dataframe(
    df: pd.DataFrame,
    llm_task: BaseLLMTask[Any, Any],
    build_input: Callable[[Dict[str, Any]], BaseModel],
    map_output_to_row: Callable[[Any], Dict[str, Any]],
    error_col: str | None = "llm_error",
) -> Tuple[pd.DataFrame, List[Any], List[TaskOutcome[Any]]]:
    """
    Apply a BaseLLMTask over a DataFrame, adding columns derived from the output model.

    This is a higher-level helper built on top of run_llm_task_over_rows. It:
      - Converts the DataFrame to a list of row dicts
      - Runs the LLM task over each row
      - Uses map_output_to_row(output_model) -> {col: value} to build new columns
      - Returns:
          * The augmented DataFrame
          * A list of usage objects (for cost aggregation)
          * The list of TaskOutcome objects

    Error handling / per-row failure policies are left to the caller, who can
    inspect the TaskOutcome list if needed.
    """
    

    if df.empty:
        return df, [], []

    # Represent rows as dicts so build_input is not tied to pandas types
    rows: List[Dict[str, Any]] = df.to_dict(orient="records")

    outcomes, usages = run_llm_task_over_rows(rows, llm_task, build_input)

    # Determine columns from the first successful output
    new_cols: Dict[str, List[Any]] = {}
    num_rows = len(outcomes)

    for idx, outcome in enumerate(outcomes):
        if outcome.ok and outcome.output is not None:
            values = map_output_to_row(outcome.output)
            # Initialize storage for any new columns
            for col in values.keys():
                if col not in new_cols:
                    new_cols[col] = [None] * num_rows
            for col, val in values.items():
                new_cols[col][idx] = val
        else:
            # For failed rows, we leave the default None values in place
            continue

    # 2) Build a generic error column if requested
    if error_col is not None:
        errors: list[Optional[str]] = []
        for outcome in outcomes:
            if outcome.ok:
                errors.append(None)
            else:
                err = outcome.metadata.get("error") if outcome.metadata else None
                errors.append(err or "LLM call failed")
        df[error_col] = errors

    for col, values in new_cols.items():
        df[col] = values

    return df, usages, outcomes

