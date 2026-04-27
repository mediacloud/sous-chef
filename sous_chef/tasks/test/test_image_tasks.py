import unittest
import os
from ..image_tasks import _parse_out_top_image

from sous_chef.tasks.sentiment_tasks import add_targeted_sentiment

FIXTURE_URL = "https://www.newsbreak.com/tulsa-ok-daily-brief-375443863/4513321614557-casey-means-questioned-on-vaccine-views-in-hearing"
VACCINE_FIXTURE = os.path.join(
    os.path.dirname(__file__),
    "top-image-story.html"
)
EXPECTED_IMAGE_URL = 'https://img.particlenews.com/img/id/0zaREK_19BjF4wu00'

class TestExtractSentences(unittest.TestCase):

    def test_parse_out_top_image(self):
        sample_size = 5
        with open(VACCINE_FIXTURE, 'r') as f:
            html_text = f.read()

        result = _parse_out_top_image(dict(final_url=FIXTURE_URL, original_url=FIXTURE_URL, content=html_text))

        assert result['resolved_url'] == FIXTURE_URL
        assert result['original_url'] == FIXTURE_URL
        assert result['top_image_url'] == EXPECTED_IMAGE_URL
