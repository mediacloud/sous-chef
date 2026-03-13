import unittest
from datetime import date

from sous_chef.flows.full_text_download_flow import (
    full_text_download_flow,
    FullTextDownloadParams,
)


class TestFullTextDownloadFlow(unittest.TestCase):
    def test_params_include_dedup_strategy(self):
        """
        Basic smoke test: constructing params should support dedup_strategy field.
        """
        params = FullTextDownloadParams(
            query="test",
            collection_ids=[],
            source_ids=[],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 2),
        )

        # Note: this test does not actually hit the MediaCloud API; in real
        # environments you would mock query_online_news. Here we only assert
        # that the flow can be imported and that params schema includes our
        # dedup strategy field (coming from MediacloudQuery).
        self.assertTrue(hasattr(params, "dedup_strategy"))


if __name__ == "__main__":
    unittest.main()

