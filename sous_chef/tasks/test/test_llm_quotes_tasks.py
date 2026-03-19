import unittest
import pandas as pd
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
            quote_count_correct = len(output.quotes) == len(test_story['quotes'])
            # check if all the quotes match
            if quote_count_correct:
                attribution_correct = 0
                pronoun_correct = 0
                for idx, llm_quote in enumerate(output.quotes):
                    if test_story['quotes'][idx]['speaker'] in llm_quote.speaker:
                        attribution_correct += 1
                    if llm_quote.pronoun == test_story['quotes'][idx]['pronoun']:
                        pronoun_correct += 1
                print(f"Story: {story_idx}: {attribution_correct}/{len(test_story['quotes'])} attributed right, "
                      f"{pronoun_correct}/{len(test_story['quotes'])} pronouns right, ")
            else:
                print(f"Story: {story_idx}: found {len(output.quotes)}, expected {len(test_story['quotes'])}")


