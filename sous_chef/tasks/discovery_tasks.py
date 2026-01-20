from prefect import task
import mediacloud.api
import mediacloud.error
from ..secrets import get_mediacloud_api_key
import pandas as pd
from datetime import date
from typing import List
import requests
import time


# Tasks use it
@task
def query_online_news(
    	query: str,
    	start_date: date,
    	end_date: date,
        collection_ids: List[int] = [],
        source_ids: List[int] = [],
    	# Clean signature!
		) -> pd.DataFrame:

    api_key = get_mediacloud_api_key()
    mc_search = mediacloud.api.SearchApi(api_key)

    stories = []
    token = None
    more_stories = True
    while more_stories:
        try:
            page, token = mc_search.story_list(
                query, 
                start_date=start_date, 
                end_date=end_date, 
                collection_ids=collection_ids, 
                source_ids=source_ids,
                expanded=True
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
            raise
        df = pd.DataFrame.from_records(page)
        stories.append(df)
        more_stories = token is not None

    return pd.concat(stories)
