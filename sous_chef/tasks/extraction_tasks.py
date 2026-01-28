"""
Entity extraction tasks.

Extract named entities from texts using SpaCy NER models.
"""
from typing import List, Dict, Optional
from prefect import task
import pandas as pd
from collections import Counter
from .utils import add_column_from_function

# Lazy import to avoid requiring spacy at module load time
try:
    import spacy_download
except ImportError:
    spacy_download = None


def extract_entities_row(
    text: str,
    model: str = "en_core_web_sm"
) -> List[Dict[str, str]]:
    """
    Extract named entities from a single text using SpaCy NER.
    
    This is the core row-processing function.
    
    Args:
        text: Text to extract entities from
        model: SpaCy model name (e.g., "en_core_web_sm", "en_core_web_lg")
        
    Returns:
        List of entity dictionaries, each with "text" and "type" keys.
        Example: [{"text": "Apple", "type": "ORG"}, {"text": "New York", "type": "GPE"}]
    """
    if spacy_download is None:
        raise ImportError(
            "spacy-download is required for entity extraction. "
            "Install it with: pip install spacy-download"
        )
    
    nlp = spacy_download.load_spacy(model)
    document = nlp(text)
    
    entities = [
        {"text": ent.text, "type": ent.label_}
        for ent in document.ents
    ]
    
    return entities


@task
def extract_entities(
    df: pd.DataFrame,
    text_column: str = "text",
    model: str = "en_core_web_sm"
) -> pd.DataFrame:
    """
    Extract named entities from DataFrame texts using SpaCy NER.
    
    Adds an 'entities' column to the DataFrame containing lists of entity
    dictionaries for each row. Each entity dict has "text" and "type" keys.
    This keeps entities associated with their source text.
    
    Args:
        df: DataFrame with text column
        text_column: Name of column containing text
        model: SpaCy model name (e.g., "en_core_web_sm", "en_core_web_lg")
        
    Returns:
        DataFrame with 'entities' column added
        
    Example:
        articles = query_online_news(...)
        # articles has: text, language columns
        
        articles = extract_entities(articles)
        # articles now has: text, language, entities columns
        # entities[i] contains entities extracted from text[i]
        # Each entity is {"text": "...", "type": "ORG"} etc.
    """
    return add_column_from_function(
        df,
        extract_entities_row,
        input_cols=[text_column],
        output_col="entities",
        model=model
    )


@task
def top_n_entities(
    df: pd.DataFrame,
    entities_column: str = "entities",
    top_n: int = 10,
    filter_type: Optional[str] = None,
    sort_by: str = "total"
) -> pd.DataFrame:
    """
    Find the top-n entities across all documents, with optional filtering by entity type.
    
    This task aggregates entities from a column containing lists of entity dictionaries.
    It can filter by entity type (e.g., "PERSON", "ORG", "GPE") and sort by either
    total count or percentage of articles containing each entity.
    
    Args:
        df: Input DataFrame with entities column
        entities_column: Name of column containing lists of entity dicts
        top_n: Number of top entities to return (-1 for all)
        filter_type: Optional entity type to filter by (e.g., "PERSON", "ORG", "GPE")
        sort_by: How to sort results - "total" (by total count) or "percentage" (by % of articles)
        
    Returns:
        DataFrame with columns:
        - entity: The entity text
        - type: The entity type (if filter_type is None, this will be the most common type)
        - count: Total number of times the entity appears across all documents
        - appearance_percent: Percentage of documents containing this entity
        - document_count: Number of documents containing this entity
        
        Results are sorted by the specified method and limited to top_n rows.
        
    Example:
        articles = extract_entities(articles)
        # articles has: text, entities columns
        # entities contains lists like [{"text": "Apple", "type": "ORG"}, ...]
        
        top_orgs = top_n_entities(articles, filter_type="ORG", top_n=10)
        # Returns top 10 organizations
        
        top_entities = top_n_entities(articles, top_n=20, sort_by="percentage")
        # Returns top 20 entities sorted by how many articles mention them
    """
    if df.empty:
        return pd.DataFrame(columns=["entity", "type", "count", "appearance_percent", "document_count"])
    
    if entities_column not in df.columns:
        raise ValueError(
            f"Column '{entities_column}' not found in DataFrame. "
            f"Available columns: {list(df.columns)}"
        )
    
    # Counters for total occurrences and document appearances
    entity_total_count = Counter()  # Total count across all documents
    entity_doc_count = Counter()    # Number of documents containing each entity
    entity_types = {}                # Track most common type for each entity
    
    number_of_documents = len(df)
    
    # Process each row's entities
    for idx, row_entities in df[entities_column].items():
        if row_entities is None:
            continue
        
        # Handle single dict case (convert to list)
        if isinstance(row_entities, dict):
            row_entities = [row_entities]
        
        # Handle empty lists
        if not isinstance(row_entities, list) or len(row_entities) == 0:
            continue
        
        # Filter by type if specified
        if filter_type:
            row_entities = [
                e for e in row_entities
                if isinstance(e, dict) and e.get("type") == filter_type
            ]
        
        # Track unique entities in this document for appearance counting
        unique_entities_in_doc = set()
        
        # Count all entity occurrences
        for entity in row_entities:
            if not isinstance(entity, dict):
                continue
            
            entity_text = entity.get("text")
            entity_type = entity.get("type")
            
            if entity_text is None:
                continue
            
            # Count total occurrences
            entity_total_count[entity_text] += 1
            
            # Track entity type (keep most common type)
            if entity_text not in entity_types:
                entity_types[entity_text] = Counter()
            entity_types[entity_text][entity_type] += 1
            
            # Track for document appearance (count once per document)
            unique_entities_in_doc.add(entity_text)
        
        # Count document appearances (each entity counted once per document)
        for entity_text in unique_entities_in_doc:
            entity_doc_count[entity_text] += 1
    
    # Get most common type for each entity
    entity_type_map = {
        entity: types.most_common(1)[0][0]
        for entity, types in entity_types.items()
    }
    
    # Build results
    if len(entity_total_count) == 0:
        return pd.DataFrame(columns=["entity", "type", "count", "appearance_percent", "document_count"])
    
    # Sort entities
    if sort_by == "total":
        sorted_entities = [e[0] for e in entity_total_count.most_common(top_n if top_n > 0 else None)]
    elif sort_by == "percentage":
        sorted_entities = [e[0] for e in entity_doc_count.most_common(top_n if top_n > 0 else None)]
    else:
        raise ValueError(f"sort_by must be 'total' or 'percentage', got '{sort_by}'")
    
    # Build result DataFrame
    results = []
    for entity_text in sorted_entities:
        total_count = entity_total_count[entity_text]
        doc_count = entity_doc_count[entity_text]
        appearance_percent = (doc_count / number_of_documents * 100) if number_of_documents > 0 else 0.0
        # If filter_type is specified, all entities have that type; otherwise use most common type
        entity_type = filter_type if filter_type else entity_type_map.get(entity_text, "UNKNOWN")
        
        results.append({
            "entity": entity_text,
            "type": entity_type,
            "count": total_count,
            "appearance_percent": appearance_percent,
            "document_count": doc_count
        })
    
    return pd.DataFrame(results)
