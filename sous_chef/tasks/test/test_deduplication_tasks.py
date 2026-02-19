import unittest
import pandas as pd
import os
from ..deduplication_tasks import _dededupe

FIXTURE_1 = os.path.join(os.path.dirname(__file__), "mc-onlinenews-mediacloud-20260206194351-content.csv")


class TestExtractSentences(unittest.TestCase):

    def setUp(self):
        self._df = pd.read_csv(FIXTURE_1)

    def test_remove_duplicates(self):
        self.assertTrue(len(self._df) == 417)
        deduped_df = _dededupe(self._df, 'title', 'media_name')
        self.assertTrue(len(deduped_df) == 384)


if __name__ == "__main__":
    unittest.main()
