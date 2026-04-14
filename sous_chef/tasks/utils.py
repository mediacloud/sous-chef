"""
Utilities for working with DataFrames in tasks.

Common patterns for applying functions to DataFrame rows and adding results as columns,
plus helpers for running LLM tasks over DataFrames.
"""
from typing import Callable, List, Tuple, Any, Dict, Optional
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
    map_output_to_row: Callable[[Any], Dict[str, Any] | List[Dict[str, Any]]],
    error_col: str | None = "llm_error",
) -> Tuple[pd.DataFrame, List[Any], List[TaskOutcome[Any]]]:
    """
    Apply a BaseLLMTask over a DataFrame and map output models back to rows.

    Returns ``(result_df, usages, outcomes)``. ``usages`` aggregates provider
    usage for cost summaries. ``outcomes`` has one entry per **input** row (same
    order as ``df``), even when ``result_df`` has a different length in list mode.

    ``map_output_to_row`` return shapes:

    - **Dict** — one output row per input row. The input frame is updated in
      place (on the copied ``df`` passed in): new columns from the dict,
      optional ``error_col`` for failures, row count and index match the input.

    - **List[dict]** — zero or more output rows per input row. Each dict is
      merged onto a **copy of the full input row** (all columns from ``df``),
      so identifiers such as story or article ids remain on every exploded row
      unless a mapping key collides with an existing column name.

    NB: If any successful row uses list-shaped output, the whole run uses list
    mode; dict outputs from other rows are treated as a single-element list.
    Additionally, In dict mode, failed rows stay in the frame with ``error_col``
    set. In list mode, failed rows produce **no** rows in ``result_df``; use
    ``outcomes`` to see errors. 
    A successful call that returns ``[]`` also
    produces no rows for that input (distinct from failure: check ``outcomes``).

    """


    if df.empty:
        return df, [], []

    # Represent rows as dicts so build_input is not tied to pandas types
    rows: List[Dict[str, Any]] = df.to_dict(orient="records")

    outcomes, usages = run_llm_task_over_rows(rows, llm_task, build_input)

    mapped_values: List[Optional[Dict[str, Any] | List[Dict[str, Any]]]] = []
    list_mode = False

    for idx, outcome in enumerate(outcomes):
        if not (outcome.ok and outcome.output is not None):
            mapped_values.append(None)
            continue

        mapped = map_output_to_row(outcome.output)

        if isinstance(mapped, dict):
            mapped_values.append(mapped)
            continue

        if isinstance(mapped, list):
            for item_idx, item in enumerate(mapped):
                if not isinstance(item, dict):
                    raise ValueError(
                        f"map_output_to_row returned list item of type "
                        f"{type(item).__name__} at row {idx}, item {item_idx}; "
                        "expected dict"
                    )
            list_mode = True
            mapped_values.append(mapped)
            continue

        raise ValueError(
            f"map_output_to_row must return dict or list[dict], "
            f"got {type(mapped).__name__} at row {idx}"
        )

    # Dict mode preserves the previous 1:1 behavior and output shape.
    if not list_mode:
        new_cols: Dict[str, List[Any]] = {}
        num_rows = len(outcomes)

        for idx, mapped in enumerate(mapped_values):
            if isinstance(mapped, dict):
                for col in mapped.keys():
                    if col not in new_cols:
                        new_cols[col] = [None] * num_rows
                for col, val in mapped.items():
                    new_cols[col][idx] = val

        if error_col is not None:
            errors: List[Optional[str]] = []
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

    # List mode expands each input row into zero or more output rows.
    expanded_rows: List[Dict[str, Any]] = []
    expanded_output_cols: set[str] = set()

    for idx, (row, outcome, mapped) in enumerate(zip(rows, outcomes, mapped_values)):
        if not (outcome.ok and outcome.output is not None) or mapped is None:
            continue

        row_mappings = mapped if isinstance(mapped, list) else [mapped]

        for output_values in row_mappings:
            if not isinstance(output_values, dict):
                raise ValueError(
                    f"map_output_to_row must return dict items in list mode, "
                    f"got {type(output_values).__name__} at row {idx}"
                )
            expanded_output_cols.update(output_values.keys())
            new_row = dict(row)
            new_row.update(output_values)
            if error_col is not None:
                new_row[error_col] = None
            expanded_rows.append(new_row)

    base_cols = list(df.columns)
    extra_cols = [col for col in expanded_output_cols if col not in base_cols]
    output_cols = [*base_cols, *extra_cols]
    if error_col is not None and error_col not in output_cols:
        output_cols.append(error_col)

    expanded_df = pd.DataFrame(expanded_rows, columns=output_cols)

    return expanded_df, usages, outcomes
