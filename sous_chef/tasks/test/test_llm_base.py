import os
import unittest
from unittest.mock import MagicMock, patch

import httpx
from groq import BadRequestError
from pydantic import BaseModel

from ..llm_base import GroqClient


class _Output(BaseModel):
    answer: str


def _bad_request(code: str) -> BadRequestError:
    response = httpx.Response(400, request=httpx.Request("POST", "https://api.groq.com"))
    body = {"error": {"message": "test error", "type": "invalid_request_error", "code": code}}
    return BadRequestError("test error", response=response, body=body)


def _wrapped(error: Exception) -> Exception:
    # instructor re-raises provider errors as its own exception, chained via __cause__
    wrapper = RuntimeError(str(error))
    wrapper.__cause__ = error
    return wrapper


class TestGroqClientRetry(unittest.TestCase):
    """Mocked tests (no network) for retrying Groq's json_validate_failed errors."""

    def setUp(self):
        # a real key isn't needed since the instructor client is mocked out
        with patch.dict(os.environ, {"GROQ_API_KEY": os.environ.get("GROQ_API_KEY", "test-key")}):
            self._client = GroqClient()
        self._client.client = MagicMock()

    def test_retries_json_validate_failed(self):
        self._client.client.create.side_effect = [
            _bad_request("json_validate_failed"),
            _wrapped(_bad_request("json_validate_failed")),
            _Output(answer="ok"),
        ]
        result, _ = self._client.execute("prompt", _Output, max_retries=3)
        assert result.answer == "ok"
        assert self._client.client.create.call_count == 3

    def test_gives_up_after_max_retries(self):
        self._client.client.create.side_effect = _wrapped(_bad_request("json_validate_failed"))
        with self.assertRaises(RuntimeError):
            self._client.execute("prompt", _Output, max_retries=2)
        assert self._client.client.create.call_count == 3
        # usage hook is unregistered even when the call fails
        self._client.client.off.assert_called_once()

    def test_other_bad_requests_not_retried(self):
        self._client.client.create.side_effect = _bad_request("model_not_found")
        with self.assertRaises(BadRequestError):
            self._client.execute("prompt", _Output, max_retries=3)
        assert self._client.client.create.call_count == 1
