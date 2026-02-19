from transformers import pipeline
import pandas as pd
from prefect import task
import torch

DEFAULT_TRANSFORMER_MODEL = "yangheng/deberta-v3-base-absa-v1.1" # a good one for targetted sentiment towards an entity

# Lazy import to avoid requiring spacy at module load time
try:
    import spacy_download
except ImportError:
    spacy_download = None

def add_targeted_sentiment(
    df: pd.DataFrame,
    sentiment_target: str,
    model: str = DEFAULT_TRANSFORMER_MODEL,
    device: int = -1
) -> pd.DataFrame:
    """
    Adds aspect-based sentiment columns to a DataFrame containing news sentences.

    Args:
        df:         Input DataFrame with a `sentence_text` column.
        sentiment_target:     The entity/aspect to evaluate sentiment toward (e.g. "Mamdani").
        model:      HuggingFace model identifier.
        device:     Device id for GPU (0, 1, etc.) or -1 for CPU.

    Returns:
        Original DataFrame with added `target_sentiment` and `target_sentiment_score` columns.
        Rows where the aspect is not mentioned are returned with NaN for both columns.
    """
    # Determine torch device
    if device == -1:
        torch_device = -1  # CPU
    else:
        torch_device = device if torch.cuda.is_available() else -1

    absa_pipeline = pipeline(
        "text-classification",
        model=model,
        tokenizer=model,
        device=torch_device,
        framework="pt"  # Explicitly use PyTorch framework
    )

    # Track which rows mention the aspect (case-insensitive)
    mask = df["sentence_text"].str.contains(sentiment_target, na=False, case=False)
    sentences = df.loc[mask, "sentence_text"].tolist()

    # Run inference on each sentence individually with the sentiment target
    outputs = []
    for sentence in sentences:
        result = absa_pipeline(sentence, text_pair=sentiment_target)
        outputs.append(result[0] if isinstance(result, list) else result)

    # Write results back into the correct rows
    df = df.copy()
    df["target_sentiment"] = None
    df["target_sentiment_score"] = None

    df.loc[mask, "target_sentiment"] = [o["label"] for o in outputs]
    df.loc[mask, "target_sentiment_score"] = [round(o["score"], 4) for o in outputs]

    return df


@task
def targeted_sentiment(
    df: pd.DataFrame,
    sentiment_target: str,
    model: str = DEFAULT_TRANSFORMER_MODEL,
    device: int = -1
) -> pd.DataFrame:
    """
    Prefect task wrapper for add_targeted_sentiment.
    """
    return add_targeted_sentiment(df, sentiment_target, model, device)
