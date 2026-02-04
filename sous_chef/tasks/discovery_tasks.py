from prefect import task
import mediacloud.api
import mediacloud.error
from ..secrets import get_mediacloud_api_key
import pandas as pd
from datetime import date
from typing import List
import requests
import time
from prefect.logging import get_run_logger
from ..artifacts import ArtifactResult, MediacloudQuerySummary


@task
def query_online_news(
    	query: str,
    	start_date: date,
    	end_date: date,
        collection_ids: List[int] = [],
        source_ids: List[int] = [],
    	# Clean signature!
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

    stories_df = pd.concat(stories)
    
    # Create summary artifact
    summary = MediacloudQuerySummary(
        query=query,
        start_date=start_date,
        end_date=end_date,
        collection_ids=collection_ids,
        source_ids=source_ids,
        story_count=len(stories_df)
    )
    
    return stories_df, summary
