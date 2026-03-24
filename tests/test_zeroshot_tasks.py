"""Unit tests for zero-shot classification tasks (mocked HF pipeline)."""
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from sous_chef.tasks.zeroshot_tasks import (
    add_zero_shot_classification,
    compute_zero_shot_label_counts,
    story_dataframe_for_zeroshot_csv,
    _truncate,
)


def test_truncate():
    assert _truncate("abcdef", 3) == "abc"
    assert _truncate("ab", 10) == "ab"
    assert _truncate("x", None) == "x"


def test_add_zero_shot_classification_requires_labels():
    df = pd.DataFrame({"text": ["a"]})
    with pytest.raises(ValueError, match="candidate_labels"):
        add_zero_shot_classification(df, [])


def test_add_zero_shot_classification_requires_column():
    df = pd.DataFrame({"wrong": ["a"]})
    with pytest.raises(ValueError, match="text"):
        add_zero_shot_classification(df, ["x"], text_column="text")


def test_add_zero_shot_classification_mocked():
    mock_clf = MagicMock()
    mock_clf.return_value = {"labels": ["politics", "economy"], "scores": [0.91, 0.12]}

    df = pd.DataFrame({"text": ["hello world", "", None]})

    with patch("sous_chef.tasks.zeroshot_tasks.pipeline", return_value=mock_clf):
        out = add_zero_shot_classification(
            df,
            ["politics", "economy"],
            text_max_chars=5,
        )

    assert len(out) == 3
    assert out.loc[0, "zeroshot_top_label"] == "politics"
    assert out.loc[1, "zeroshot_top_label"] == ""
    assert pd.isna(out.loc[1, "zeroshot_top_score"])
    labels = json.loads(out.loc[0, "zeroshot_labels_json"])
    assert labels == ["politics", "economy"]
    mock_clf.assert_called_once()
    first_kw = mock_clf.call_args
    assert first_kw[0][0] == "hello"


def test_add_zero_shot_passing_threshold_column_mocked():
    mock_clf = MagicMock()
    mock_clf.return_value = {"labels": ["politics", "economy"], "scores": [0.91, 0.12]}

    df = pd.DataFrame({"text": ["hello"]})

    with patch("sous_chef.tasks.zeroshot_tasks.pipeline", return_value=mock_clf):
        out = add_zero_shot_classification(
            df,
            ["politics", "economy"],
            passing_score_threshold=0.5,
        )

    passed = json.loads(out.loc[0, "zeroshot_labels_passing_threshold_json"])
    assert passed == ["politics"]


def test_compute_zero_shot_label_counts_top_label():
    df = pd.DataFrame(
        {
            "zeroshot_top_label": ["a", "b", "", "a"],
            "zeroshot_labels_json": ['["a"]', '["b"]', "[]", '["a"]'],
            "zeroshot_scores_json": ["[1]", "[1]", "[]", "[1]"],
        }
    )
    counts, no_pred = compute_zero_shot_label_counts(
        df, ["a", "b", "c"], summary_score_threshold=None
    )
    assert counts == [2, 1, 0]
    assert no_pred == 1


def test_compute_zero_shot_label_counts_threshold():
    df = pd.DataFrame(
        {
            "zeroshot_labels_json": [
                '["a","b"]',
                '["a","b"]',
                '["a","b"]',
            ],
            "zeroshot_scores_json": [
                "[0.9,0.2]",
                "[0.1,0.8]",
                "[0.2,0.3]",
            ],
            "zeroshot_top_label": ["a", "b", "a"],
        }
    )
    counts, no_pred = compute_zero_shot_label_counts(
        df, ["a", "b"], summary_score_threshold=0.5
    )
    assert counts == [1, 1]
    assert no_pred == 1


def test_story_dataframe_for_zeroshot_csv_drops_text():
    df = pd.DataFrame(
        {
            "story_id": [1],
            "title": ["t"],
            "text": ["SECRET"],
            "zeroshot_top_label": ["politics"],
        }
    )
    out = story_dataframe_for_zeroshot_csv(df)
    assert "text" not in out.columns
    assert "story_id" in out.columns
    assert "zeroshot_top_label" in out.columns
