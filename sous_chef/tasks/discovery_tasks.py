from prefect import task
import mediacloud.api
import mediacloud.error
from ..secrets import get_mediacloud_api_key
import pandas as pd
from datetime import date
from typing import List
import requests
import time
from ..artifacts import ArtifactResult, MediacloudQuerySummary, ArticleDeduplicationSummary
from ..params.mediacloud_query import DedupStrategy
from ..tasks.export_tasks import csv_to_b2
from ..utils import create_url_safe_slug, get_logger
from .deduplication_tasks import deduplicate_articles


@task
def query_online_news(
    query: str,
    start_date: date,
    end_date: date,
    collection_ids: List[int] = [],
    source_ids: List[int] = [],
    dedup_strategy: DedupStrategy = DedupStrategy.none,
    upload_dedup_summary: bool = False,
) -> ArtifactResult[pd.DataFrame]:
    """
    Query MediaCloud for news articles matching a search query.
    
    Returns:
        ArtifactResult[pd.DataFrame]: Tuple of (DataFrame, MediacloudQuerySummary)
        
        - First element: DataFrame containing articles matching the query
        - Second element: MediacloudQuerySummary artifact with query context and statistics
        
    Example:
        articles, query_summary = query_online_news(
            query="climate change",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
    """
    logger = get_logger()
    api_key = get_mediacloud_api_key()
    mc_search = mediacloud.api.SearchApi(api_key)
    stories = []
    pagination_token = None
    more_stories = True
    while more_stories:
        try:
            page, pagination_token = mc_search.story_list(
                query, 
                start_date=start_date, 
                end_date=end_date, 
                collection_ids=collection_ids, 
                source_ids=source_ids,
                expanded=True,
                pagination_token=pagination_token
            )
            #time.sleep(5)
        except requests.exceptions.JSONDecodeError as e:
            # Handle case where API returns non-JSON response (e.g., HTML error page)
            raise RuntimeError(
                f"MediaCloud API returned a non-JSON response. "
                f"This usually indicates an API error (authentication, rate limit, etc.). "
                f"Original error: {str(e)}"
            ) from e
        except mediacloud.error.APIResponseError as e:
            # Re-raise API errors as-is
            raise e
        df = pd.DataFrame.from_records(page)
        stories.append(df)
        more_stories = pagination_token is not None

    # Concatenate all pages into a single DataFrame and reset index to avoid
    # duplicate indices across pages, which can break downstream dedup logic.
    stories_df = pd.concat(stories, ignore_index=True)

    dedup_summary = None
    duplicates_file_artifact = None
    if dedup_strategy != DedupStrategy.none:
        if dedup_strategy == DedupStrategy.title:
            deduped_df, dedup_stats_df = deduplicate_articles(
                stories_df,
                dedup_by_title=True,
                dedup_by_text=False,
                dedup_title_column="title",
                dedup_text_column="text",
                dedup_date_column="publish_date",
                keep_earliest=True,
                return_stats=True,
            )
        elif dedup_strategy == DedupStrategy.title_source:
            # Drop duplicates by (title, media_name) within a source
            deduped_df = stories_df.drop_duplicates(
                subset=["title", "media_name"], keep="first"
            )
            dup_mask = ~stories_df.index.isin(deduped_df.index)
            dedup_stats_df = stories_df[dup_mask].copy()
        else:
            deduped_df = stories_df
            dedup_stats_df = pd.DataFrame()

        # Optionally upload detailed duplicates as a CSV when requested
        if upload_dedup_summary and not dedup_stats_df.empty:
            slug = create_url_safe_slug(query)
            object_name = f"sous-chef-output/DATE/mediacloud-dedup-{slug}.csv"
            logger.info(f"Uploading deduplication summary to B2: {object_name}")
            _, duplicates_file_artifact = csv_to_b2(
                dedup_stats_df,
                object_name=object_name,
                add_date_slug=True,
                ensure_unique=True,
            )

        dedup_summary = ArticleDeduplicationSummary(
            input_story_count=len(stories_df),
            deduplicated_story_count=len(deduped_df),
            duplicate_story_count=len(dedup_stats_df),
            strategy=dedup_strategy.value,
            duplicates_file=duplicates_file_artifact,
        )
        stories_df = deduped_df
    
    # Create summary artifact
    summary = MediacloudQuerySummary(
        query=query,
        start_date=start_date,
        end_date=end_date,
        collection_ids=collection_ids,
        source_ids=source_ids,
        story_count=len(stories_df),
        dedup_summary=dedup_summary,
    )
    
    return stories_df, summary
