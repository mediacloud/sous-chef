import hashlib
from typing import Optional, Tuple, List

import pandas as pd
from prefect import task


def _dededupe(df: pd.DataFrame, title_column: str, source_name_column: str) -> pd.DataFrame:
    # this is a little silly...
    return df.drop_duplicates(subset=[title_column, source_name_column])


@task
def deduplicate_on_title_source(
    df: pd.DataFrame,
    title_column: str = "title",
    source_name_column: str = "media_name",
) -> pd.DataFrame:
    """
    Naively deduplicate stories based on media source and title.

    Args:
        df: list of stories that might include duplicates (full story_list)
        title_column: name of column with title of story
        source_name_column: name of column with media source name

    Returns:
        New DataFrame with duplicates removed, keeping only the first occurrence of each
        unique title-source combination.
    """
    return _dededupe(df, title_column, source_name_column)


def _normalize_text(value: str) -> str:
    if not isinstance(value, str):
        return ""
    # very lightweight normalization: strip and collapse whitespace, lowercase
    collapsed = " ".join(value.split())
    return collapsed.lower()


def _hash_text(value: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        return ""
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def _build_dedup_key_columns(
    df: pd.DataFrame,
    title_column: Optional[str],
    text_column: Optional[str],
    use_title: bool,
    use_text: bool,
) -> Tuple[pd.DataFrame, List[str]]:
    """Add helper columns to build a deduplication key and return the key columns."""
    key_columns: List[str] = []
    working = df.copy()

    if use_title and title_column in working.columns:
        norm_title_col = "_dedup_norm_title"
        working[norm_title_col] = working[title_column].astype(str).map(_normalize_text)
        key_columns.append(norm_title_col)

    if use_text and text_column in working.columns:
        hash_text_col = "_dedup_text_hash"
        working[hash_text_col] = working[text_column].astype(str).map(_hash_text)
        key_columns.append(hash_text_col)

    if not key_columns:
        # fall back to no-op: every row is its own group
        working["_dedup_row_index"] = range(len(working))
        key_columns.append("_dedup_row_index")

    return working, key_columns


def _select_earliest_in_group(
    df: pd.DataFrame,
    group_key_cols: List[str],
    date_column: Optional[str],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split the frame into kept and duplicate rows given group key columns
    and an optional date column for ordering.
    """
    sort_cols: List[str] = []
    if date_column and date_column in df.columns:
        sort_cols.append(date_column)
    # always add original index as a stable tiebreaker
    df = df.reset_index().rename(columns={"index": "_dedup_orig_index"})
    sort_cols.append("_dedup_orig_index")

    df_sorted = df.sort_values(sort_cols)
    # mark first row in each group as kept
    group_obj = df_sorted.groupby(group_key_cols, dropna=False, sort=False)
    keep_mask = group_obj.cumcount() == 0

    kept = df_sorted[keep_mask].copy()
    dups = df_sorted[~keep_mask].copy()

    # restore original index for downstream compatibility; keep helper columns
    # so that stats computation can still access group keys.
    kept.set_index("_dedup_orig_index", inplace=True)
    dups.set_index("_dedup_orig_index", inplace=True)
    return kept, dups


def deduplicate_articles(
    df: pd.DataFrame,
    *,
    dedup_by_title: bool = True,
    dedup_by_text: bool = False,
    dedup_title_column: str = "title",
    dedup_text_column: str = "content",
    dedup_date_column: str = "publish_date",
    keep_earliest: bool = True,
    return_stats: bool = False,
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """
    Deduplicate articles based on configurable title/text keys.

    This function is intentionally not a Prefect task so that flows can choose
    whether to wrap it or call it directly.

    Args:
        df: Article DataFrame, typically from query_online_news.
        dedup_by_title: If true, include normalized title in the deduplication key.
        dedup_by_text: If true, include normalized text hash in the deduplication key.
        dedup_title_column: Column name for titles.
        dedup_text_column: Column name for article text content.
        dedup_date_column: Column name used to choose the earliest article in a group.
        keep_earliest: If true, keep earliest article in each group; if false, behavior
            is currently identical (earliest is still kept) but kept for future extension.
        return_stats: If true, also return a DataFrame describing dropped duplicates.

    Returns:
        Tuple of:
            - deduplicated DataFrame
            - optional statistics DataFrame of dropped duplicates (or None)
    """
    if df is None or df.empty:
        return df, pd.DataFrame() if return_stats else None

    working, key_cols = _build_dedup_key_columns(
        df,
        title_column=dedup_title_column,
        text_column=dedup_text_column,
        use_title=dedup_by_title,
        use_text=dedup_by_text,
    )

    # select earliest per group (keep_earliest currently required semantics)
    kept, dups = _select_earliest_in_group(
        working,
        group_key_cols=key_cols,
        date_column=dedup_date_column if keep_earliest else None,
    )

    # When stats are not requested, return a cleaned-up kept frame without
    # internal helper columns.
    if not return_stats:
        drop_cols = [c for c in kept.columns if c.startswith("_dedup_")]
        kept = kept.drop(columns=drop_cols, errors="ignore")
        return kept, None

    if dups.empty:
        stats_df = pd.DataFrame()
    else:
        # For stats, keep a compact view of duplicates and which story was kept
        # reattach group key columns for readability
        stats_cols: List[str] = []
        for col in key_cols:
            if col in working.columns:
                stats_cols.append(col)
        base_cols = [c for c in ["stories_id", dedup_title_column, dedup_date_column, "url", "media_name"] if c in df.columns]

        kept_lookup = kept.copy()
        # build a mapping from group keys to kept story metadata (if IDs exist)
        merge_on = key_cols
        dups_with_keys = working.loc[dups.index, key_cols].copy()
        dups_with_keys["__dup_index"] = dups.index

        kept_for_merge = kept_lookup.copy()
        kept_for_merge["__kept_marker"] = True

        merged = dups_with_keys.merge(
            kept_for_merge,
            on=key_cols,
            how="left",
            suffixes=("", "_kept"),
        )
        merged.set_index("__dup_index", inplace=True)

        # build final stats frame from original df plus mapping to kept
        stats_df = df.loc[merged.index].copy()
        # annotate with group size-ish info is possible
        # re-add keys for analysis
        for col in stats_cols:
            if col in working.columns:
                stats_df[col] = working.loc[stats_df.index, col]

        if "stories_id" in kept_for_merge.columns:
            stats_df["kept_stories_id"] = merged.get("stories_id", pd.Series(index=merged.index))

    # Drop helper columns from the returned kept frame
    drop_cols = [c for c in kept.columns if c.startswith("_dedup_")]
    kept = kept.drop(columns=drop_cols, errors="ignore")

    return kept, stats_df

