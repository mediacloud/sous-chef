import unittest
import os
import json

from sous_chef.tasks.llm_quotes import ArticleQuotesOutput, _QUOTE_EXTRACTION_PROMPT
from ..llm_base import GroqClient
from ...params import GroqModelName

FIXTURE_1 = os.path.join(
    os.path.dirname(__file__),
    "quote-samples.json",
)


class TestExtractQuotes(unittest.TestCase):

    def setUp(self):
        self._client = GroqClient(model_name=GroqModelName.llama)

    def test_static_example(self):
        # load sample stories from quote-samples.csv into a data frame
        with open(FIXTURE_1, "r") as f:
            samples = json.load(f)
        # run the `text` from each story through the LLM function from the task
        for story_idx, test_story in enumerate(samples):
            # can't do .format call here because there are JSON format note in the prompt
            story_prompt = _QUOTE_EXTRACTION_PROMPT.replace("{text}", test_story['text'])
            output, usage = self._client.execute(story_prompt, ArticleQuotesOutput)
            for q in output.quotes:
                assert len(q.quote) > 0  # there is text that got extracted
