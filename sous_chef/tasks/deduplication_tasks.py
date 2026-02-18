import pandas as pd
from prefect import task


def _dededupe(df: pd.DataFrame, title_column: str, source_name_column: str) -> pd.DataFrame:
    # this is a little silly...
    return df.drop_duplicates(subset=[title_column, source_name_column])


@task
def deduplicate_on_title_source(
        df: pd.DataFrame,
        title_column: str = "title",
        source_name_column: str = "media_name"
) -> pd.DataFrame:
    """
    Naively deduplicate stories based on media source and title.

    Args:
        df: list of stories that might include duplicates (full story_list)
        title_column: name of column with title of story
        source_name_column: name of column with media source name

    Returns:
        New DataFrame with duplicates removed, keeping only the first occurrence of each unique title-source
        combination.
    """
    return _dededupe(df, title_column, source_name_column)
