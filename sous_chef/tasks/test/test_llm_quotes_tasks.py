import unittest
import os
import json

from sous_chef.prompts import load_prompt
from sous_chef.tasks.llm_quotes import ArticleQuotesOutput
from ..llm_base import GroqClient
from ...params import GroqModelName

FIXTURE_1 = os.path.join(
    os.path.dirname(__file__),
    "quote-samples.json",
)


class TestExtractQuotes(unittest.TestCase):

    def setUp(self):
        self._client = GroqClient(model_name=GroqModelName.gpt_oss_120b)

    def test_static_example(self):
        # load sample stories from quote-samples.csv into a data frame
        with open(FIXTURE_1, "r") as f:
            samples = json.load(f)
        base_prompt = load_prompt("quotes", "v2-sauti-health.txt")
        # first story has many quotes
        story_prompt0 = base_prompt.replace("{text}", samples[0]["text"])
        output0, usage0 = self._client.execute(story_prompt0, ArticleQuotesOutput)
        assert len(output0.quotes) > 0
        for q in output0.quotes:
            assert len(q.quote) > 0  # there is text that got extracted
        # second one has none
        story_prompt1 = base_prompt.replace("{text}", samples[1]["text"])
        output1, usage1 = self._client.execute(story_prompt1, ArticleQuotesOutput)
        assert len(output1.quotes) == 0

