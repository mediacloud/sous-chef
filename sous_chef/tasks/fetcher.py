import collections
import logging
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

import scrapy
import scrapy.crawler as crawler
from scrapy.http import Response
from scrapy.utils.reactor import install_reactor
from twisted.internet import defer
from mcmetadata.webpages import DEFAULT_USER_AGENT

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class UrlSpider(scrapy.Spider):
    name: str = "urlspider"

    custom_settings: Dict[str, Any] = {
        "COOKIES_ENABLED": False,
        # "HTTPCACHE_ENABLED": True,  # useful to have set True locally for repetative runs while debugging code changes
        "LOG_LEVEL": "INFO",
        "CONCURRENT_REQUESTS": 64,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 5,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 64,
        "DOWNLOAD_TIMEOUT": 5,
        "USER_AGENT": DEFAULT_USER_AGENT,
    }

    def __init__(
        self,
        handle_parse: Optional[Callable],
        start_urls: List[str],
        *args: List,
        **kwargs: Dict,
    ) -> None:
        """
        Handle_parse will be called with a story:Dict object
        :param handle_parse: called with story_data dict of "content", "final_url", "original_url" keys for each story
        :param start_urls: lst of URLs to fetch
        :param args: passed to parent constructor
        :param kwargs: passed to parent constructor
        """
        super().__init__(*args, **kwargs)
        self.on_parse = handle_parse
        self.start_urls = start_urls
        logging.getLogger("scrapy").setLevel(logging.DEBUG)
        logging.getLogger("scrapy.core.engine").setLevel(logging.DEBUG)

    def parse(self, response: Response, **kwargs: Any) -> Any:
        # grab the original, undirected URL so we can relink later
        orig_url = (
            response.request.meta["redirect_urls"][0]
            if "redirect_urls" in response.request.meta
            else response.request.url
        )
        story_data = dict(
            content=response.text, final_url=response.request.url, original_url=orig_url
        )
        if self.on_parse:
            self.on_parse(story_data)
        return None


def group_urls_by_domain(urls: List[str]) -> List[List[str]]:
    """
    Groups URLs by their domain. Skips URLs that do not have extractable domains.
    """
    domain_groups = collections.defaultdict(list)

    for url in urls:
        domain = urlparse(url).netloc
        if domain:
            domain_groups[domain].append(url)

    return list(domain_groups.values())


def fetch_all_html(
    urls: List[str], handle_parse: Callable, num_spiders: int = 4
) -> None:
    if not urls:
        return

    # logging.info("=== fetch_all_html START ===")
    # logging.info(f"Processing {len(urls)} URLs")
    domain_list = group_urls_by_domain(urls)
    batches = [[] for _ in range(num_spiders)]
    for i, domain_urls in enumerate(domain_list):
        batches[i % num_spiders].extend(domain_urls)

    logging.debug(
        f"Created {len(batches)} batches, first batch has {len(batches[0])} URLs"
    )

    # Single runner for ALL spiders
    # logging.info("Install reactor...")
    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
    # logging.info("Reactor installed")
    from twisted.internet import reactor  # call after install

    # logging.info("Creating CrawlerRunner...")
    runner = crawler.CrawlerRunner()
    # logging.info("CrawlerRunner created")

    # logging.info("About to call runner.crawl()...")
    deferreds = [
        runner.crawl(UrlSpider, handle_parse=handle_parse, start_urls=batch)
        for batch in batches
        if batch
    ]
    # logging.info(f"runner.crawl() returned, created {len(deferreds)} deferreds")

    dl = defer.DeferredList(deferreds)
    # logging.info("DeferredList created")

    dl.addBoth(lambda _: reactor.stop())
    # logging.info("Added stop callback")

    # logging.info("About to call reactor.run()")
    reactor.run()
    # logging.info("=== reactor.run() completed ===")
