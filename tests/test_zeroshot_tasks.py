"""Unit tests for zero-shot classification tasks (mocked HF pipeline)."""
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from sous_chef.tasks.zeroshot.config import ZEROSHOT_HF_INFERENCE_TIMEOUT_S
from sous_chef.tasks.zeroshot_tasks import (
    add_zero_shot_classification,
    compute_zero_shot_label_counts,
    get_zeroshot_backend,
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

    with patch("sous_chef.tasks.zeroshot.local.pipeline", return_value=mock_clf):
        out = add_zero_shot_classification(
            df,
            ["politics", "economy"],
            text_max_chars=5,
            backend="local",
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


def test_get_zeroshot_backend_invalid():
    with pytest.raises(ValueError, match="Invalid"):
        get_zeroshot_backend("not_a_backend")


def test_add_zero_shot_hf_inference_mocked():
    pol = MagicMock()
    pol.label = "politics"
    pol.score = 0.91
    econ = MagicMock()
    econ.label = "economy"
    econ.score = 0.12
    mock_client = MagicMock()
    mock_client.zero_shot_classification.return_value = [pol, econ]

    df = pd.DataFrame({"text": ["hello world"]})

    with patch(
        "sous_chef.tasks.zeroshot.hf_inference._hf_token_optional",
        return_value=None,
    ), patch(
        "sous_chef.tasks.zeroshot.hf_inference.get_hf_bill_to",
        return_value="my-org",
    ), patch(
        "sous_chef.tasks.zeroshot.hf_inference.InferenceClient",
        return_value=mock_client,
    ) as mock_inference_client:
        out = add_zero_shot_classification(
            df,
            ["politics", "economy"],
            backend="hf_inference",
        )

    mock_inference_client.assert_called_once_with(
        token=None,
        bill_to="my-org",
        timeout=ZEROSHOT_HF_INFERENCE_TIMEOUT_S,
    )
    assert out.loc[0, "zeroshot_top_label"] == "politics"
    mock_client.zero_shot_classification.assert_called_once()
    call_kw = mock_client.zero_shot_classification.call_args
    assert call_kw[0][0] == "hello world"
    assert call_kw[1]["candidate_labels"] == ["politics", "economy"]


def test_add_zero_shot_hf_inference_mocked_without_bill_to():
    pol = MagicMock()
    pol.label = "politics"
    pol.score = 0.91
    econ = MagicMock()
    econ.label = "economy"
    econ.score = 0.12
    mock_client = MagicMock()
    mock_client.zero_shot_classification.return_value = [pol, econ]

    df = pd.DataFrame({"text": ["hello world"]})

    with patch(
        "sous_chef.tasks.zeroshot.hf_inference._hf_token_optional",
        return_value=None,
    ), patch(
        "sous_chef.tasks.zeroshot.hf_inference.get_hf_bill_to",
        return_value=None,
    ), patch(
        "sous_chef.tasks.zeroshot.hf_inference.InferenceClient",
        return_value=mock_client,
    ) as mock_inference_client:
        out = add_zero_shot_classification(
            df,
            ["politics", "economy"],
            backend="hf_inference",
        )

    mock_inference_client.assert_called_once_with(
        token=None,
        timeout=ZEROSHOT_HF_INFERENCE_TIMEOUT_S,
    )
    assert out.loc[0, "zeroshot_top_label"] == "politics"
    mock_client.zero_shot_classification.assert_called_once()
    call_kw = mock_client.zero_shot_classification.call_args
    assert call_kw[0][0] == "hello world"
    assert call_kw[1]["candidate_labels"] == ["politics", "economy"]


def test_add_zero_shot_passing_threshold_column_mocked():
    mock_clf = MagicMock()
    mock_clf.return_value = {"labels": ["politics", "economy"], "scores": [0.91, 0.12]}

    df = pd.DataFrame({"text": ["hello"]})

    with patch("sous_chef.tasks.zeroshot.local.pipeline", return_value=mock_clf):
        out = add_zero_shot_classification(
            df,
            ["politics", "economy"],
            passing_score_threshold=0.5,
            backend="local",
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
    counts, no_pred, failed = compute_zero_shot_label_counts(
        df, ["a", "b", "c"], summary_score_threshold=None
    )
    assert counts == [2, 1, 0]
    assert no_pred == 1
    assert failed == 0


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
    counts, no_pred, failed = compute_zero_shot_label_counts(
        df, ["a", "b"], summary_score_threshold=0.5
    )
    assert counts == [1, 1]
    assert no_pred == 1
    assert failed == 0


def test_compute_zero_shot_label_counts_skips_inference_errors():
    df = pd.DataFrame(
        {
            "zeroshot_top_label": ["a", "unknown", "b"],
            "zeroshot_labels_json": ['["a"]', "[]", '["b"]'],
            "zeroshot_scores_json": ["[1]", "[]", "[1]"],
            "zeroshot_error": ["", "HfHubHTTPError: 504", ""],
        }
    )
    counts, no_pred, failed = compute_zero_shot_label_counts(
        df, ["a", "b"], summary_score_threshold=None
    )
    assert failed == 1
    assert counts == [1, 1]
    assert no_pred == 0


def test_zeroshot_classification_failure_details():
    from sous_chef.tasks.zeroshot.common import zeroshot_classification_failure_details

    df = pd.DataFrame(
        {
            "story_id": ["s1"],
            "title": ["Hello"],
            "zeroshot_error": ["Something broke"],
        }
    )
    details = zeroshot_classification_failure_details(df)
    assert len(details) == 1
    assert details[0]["story_id"] == "s1"
    assert details[0]["title"] == "Hello"
    assert "broke" in details[0]["error"]


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
