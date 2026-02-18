import re
import pandas as pd
from typing import List, Optional, Any, Tuple
from prefect import task
import logging

# Lazy import to avoid requiring spacy at module load time
try:
    import spacy_download
except ImportError:
    spacy_download = None


def extract_matching_sentences(
    nlp,
    text: str,
    inclusion_filters: Optional[List[re.Pattern]] = None,
) -> List[Tuple[int, str]]:
    """
    Extract sentences from text using spaCy's sentence segmentation.

    Args:
        nlp: spaCy Language object to use
        text: Text to extract keywords from
        language: 
        inclusion_filters: Optional list of regex patterns to filter sentences (only include sentences that match
        any pattern)

    Returns:
        List of tuples with sentence index and string
    """
    doc = nlp(text)
    sentences = [(idx, sent.text.strip()) for (idx, sent) in enumerate(doc.sents)]
    if inclusion_filters:
        sentences = [
            s for s in sentences
            if any(pattern.search(s[1]) for pattern in inclusion_filters)
        ]
    return sentences


@task
def matching_sentences(
    df: pd.DataFrame,
    text_column: str = "text",
    language_column: str = "language",
    model: str = "en_core_web_sm",
    inclusion_filters: Optional[List[re.Pattern]] = None
) -> pd.DataFrame:

    if spacy_download is None:
        raise ImportError(
            "spacy-download is required for sentence splitting. "
            "Install it with: pip install spacy-download"
        )

    nlp = spacy_download.load_spacy(model)

    results = []

    for index, row in df.iterrows():
        text = row[text_column]
        language = row[language_column]
        sentences = extract_matching_sentences(nlp, text, inclusion_filters) #removed language parameter for testing
        for s in sentences:
            results.append({
                "id": row["id"],
                "media_name": row["media_name"],
                "title": row["title"],
                "publish_date": row["publish_date"],
                "url": row["url"],
                "language": language,
                "sentence_id": s[0],
                "sentence_text": s[1],
            })

    return pd.DataFrame(results)
