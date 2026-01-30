"""
Lightweight tests for csv_to_b2 task logic.

These tests are intentionally simple and do not require network access.
They exercise:
- CSV generation (columns, header row)
- Object name slugging and uniqueness behavior (via dry_run)
"""
import os
from datetime import date

import pandas as pd

from sous_chef.tasks.export_tasks import csv_to_b2


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"value": "foo", "count": 1},
            {"value": "bar", "count": 2},
        ]
    )


def test_csv_to_b2_dry_run_basic_naming():
    df = _sample_df()
    # Set environment variable for bucket name (since we're not in Prefect context)
    os.environ["B2_BUCKET"] = "demo-bucket"
    try:
        result = csv_to_b2.fn(  # type: ignore[attr-defined]
            df,
            object_name="sous-chef-two/DATE/demo.csv",
            add_date_slug=True,
            ensure_unique=False,
            dry_run=True,
        )

        assert result["bucket"] == "demo-bucket"
    finally:
        # Clean up environment variable
        os.environ.pop("B2_BUCKET", None)
    assert result["object"].startswith("sous-chef-two/")
    assert result["object"].endswith("/demo.csv")
    # Ensure DATE was replaced with today's date
    today = date.today().strftime("%Y-%m-%d")
    assert today in result["object"]
    assert result["columns_saved"] == ["value", "count"]
