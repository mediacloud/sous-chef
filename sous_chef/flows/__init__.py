"""
Example flows for sous-chef demo.

These flows demonstrate the sous-chef architecture with simple examples.
"""

# Import flows to register them
from .keywords_demo_flow import keywords_demo_flow
from .entities_demo_flow import entities_demo_flow
from .matching_sentences import matching_sentences_flow
from .targeted_sentiment_flow import targeted_sentiment_flow
from .full_text_download_flow import full_text_download_flow
from .llm_demo_flow import llm_demo_flow

__all__ = [
    "keywords_demo_flow",
    "entities_demo_flow",
    "matching_sentences_flow",
    "targeted_sentiment_flow",
    "full_text_download_flow",
    "llm_demo_flow",
]
