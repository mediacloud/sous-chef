from typing import List, Dict
import pandas as pd
from prefect import task
import mcmetadata.content

import sous_chef.tasks.fetcher as fetcher

def _parse_out_top_image(response_data: Dict):
    story_info = dict(resolved_url=response_data['final_url'],
                      original_url=response_data['original_url'],
                      top_image_url=None)
    try:
        story_meta = mcmetadata.content.from_html(response_data['final_url'],
                                                  response_data['content'])
        story_info['top_image_url'] = story_meta['top_image_url']
    except Exception:
        pass  # bad parse? just return no top image
    return story_info

def fetch_top_image_info(urls: List[str]) -> List[Dict]:
    results = []

    def _handle_parse(response_data: Dict):
        # called for each story that successfully is fetched by Scrapy via callback
        nonlocal results
        story_info = _parse_out_top_image(response_data)
        results.append(story_info)

    # download them all in parallel... will take a while (make it only unique URLs first)
    fetcher.fetch_all_html(list(set(urls)), _handle_parse)
    return results

def add_top_image(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Adds the url of the "top image"; you gotta download it yourself

    Args:
        df:         Input DataFrame with a `url` column.

    Returns:
        Original DataFrame with added `top_image_url` and `resolved_url` columns.
    """

    urls = df['url'].tolist()
    top_image_results = fetch_top_image_info(urls)

    # Write results back into the correct rows
    df = df.copy()
    df['top_image_url'] = ""
    df['resolved_url'] = ""
    for story_info in top_image_results:
        mask = df['url'] == story_info['original_url']
        df.loc[mask, 'top_image_url'] = story_info['top_image_url']
        df.loc[mask, 'resolved_url'] = story_info['resolved_url']
    return df


@task
def top_image(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Prefect task wrapper for add_top_image
    """
    return add_top_image(df)
