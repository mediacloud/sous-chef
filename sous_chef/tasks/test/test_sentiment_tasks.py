import unittest
import pandas as pd
import spacy_download

from sous_chef.tasks.sentiment_tasks import add_targeted_sentiment

SENTENCES = [
    'New York City Mayor Zohran Mamdani has made "fast and free buses" a defining promise of his administration, framing the proposal as both an affordability measure and a long-overdue fix for a bus system that advocates say has been neglected for decades.',
    "Despite the concerns, even cautious observers say Mamdani's proposal has shifted the conversation.",
    "New York City Mayor Zohran Mamdani re-shared a post by New York Knicks point guard Jalen Brunson, praising the Latin artist.",
    "Yet Mamdani's first month in office has been a cascade of failures that could foreshadow disaster for Raman in LA."
]

SENTENCES_DF = pd.DataFrame({
    'sentence_text': SENTENCES
})

class TestExtractSentences(unittest.TestCase):

    def setUp(self):
        self._nlp = spacy_download.load_spacy('en_core_web_sm')

    def test_static_example(self):
        results = add_targeted_sentiment(SENTENCES_DF, sentiment_target="Mamdani")
        # Values match transformers 4.57 and 5.20; early 5.x releases (e.g. 5.3) dropped the
        # DeBERTa [CLS]/[SEP] special tokens on sentence pairs and gave different results
        expected_sentiments = ['Neutral', 'Neutral', 'Neutral', 'Negative']
        expected_scores = [0.8142, 0.6907, 0.9862, 0.9963]
        self.assertListEqual(results["target_sentiment"].tolist(), expected_sentiments)
        self.assertListEqual(results["target_sentiment_score"].tolist(), expected_scores)
