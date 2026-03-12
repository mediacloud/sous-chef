import unittest
import pandas as pd
import os

from ..deduplication_tasks import (
    _dededupe,
    deduplicate_articles,
)

FIXTURE_1 = os.path.join(
    os.path.dirname(__file__),
    "mc-onlinenews-mediacloud-20260206194351-content.csv",
)


class TestDeduplicationTasks(unittest.TestCase):
    def setUp(self):
        self._df = pd.read_csv(FIXTURE_1)

    def test_remove_duplicates_legacy_helper(self):
        # keep existing behavior on title+media_name
        self.assertEqual(len(self._df), 417)
        deduped_df = _dededupe(self._df, "title", "media_name")
        self.assertEqual(len(deduped_df), 384)

    def test_deduplicate_articles_by_title_only(self):
        # construct a tiny synthetic frame with obvious duplicates
        df = pd.DataFrame(
            [
                {"stories_id": 1, "title": "Story A", "publish_date": "2024-01-01"},
                {"stories_id": 2, "title": "Story A ", "publish_date": "2024-01-02"},
                {"stories_id": 3, "title": "Story B", "publish_date": "2024-01-01"},
            ]
        )

        deduped, stats = deduplicate_articles(
            df,
            dedup_by_title=True,
            dedup_by_text=False,
            dedup_title_column="title",
            dedup_text_column="does_not_exist",
            dedup_date_column="publish_date",
            keep_earliest=True,
            return_stats=True,
        )

        # We should keep earliest of the two Story A rows plus Story B => 2 rows.
        self.assertEqual(len(deduped), 2)
        self.assertIn(1, deduped["stories_id"].tolist())
        self.assertIn(3, deduped["stories_id"].tolist())

        # Stats should contain exactly one duplicate (stories_id == 2)
        self.assertIsNotNone(stats)
        self.assertEqual(len(stats), 1)
        self.assertIn("stories_id", stats.columns)
        self.assertEqual(stats["stories_id"].iloc[0], 2)


if __name__ == "__main__":
    unittest.main()
