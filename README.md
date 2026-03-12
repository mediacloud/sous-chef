# Sous-Chef

Sous-Chef is a data pipeline tool for building and executing data processing workflows with MediaCloud and other data sources.

## Version 3.0.0

This version introduces a new Python-based flow architecture. Flows are defined as Python functions with Pydantic parameter models, providing better type safety and easier testing compared to the previous YAML-based recipe system.

### Quick Start

```python
from sous_chef.flows import keywords_demo_flow
from sous_chef.flow import get_flow

# List available flows
from sous_chef.flow import list_flows
flows = list_flows()

# Run a flow
flow_meta = get_flow("keywords_demo")
result = flow_meta["func"].fn(params)
# result is a FlowOutput model (subclass of BaseFlowOutput) whose fields are artifacts
```

### Flow Return Types

Flows should return a FlowOutput model, not a bare dictionary. Define a model
subclassing `BaseFlowOutput` and reference it in `@register_flow`:

```python
from pydantic import BaseModel
from sous_chef.flow import register_flow, BaseFlowOutput
from sous_chef.artifacts import MediacloudQuerySummary, FileUploadArtifact

class MyFlowOutput(BaseFlowOutput):
    query_summary: MediacloudQuerySummary
    b2_artifact: FileUploadArtifact

@register_flow(
    name="my_flow",
    description="...",
    params_model=MyParams,
    output_model=MyFlowOutput,
)
def my_flow(params: MyParams) -> MyFlowOutput:
    # ... flow logic ...
    return MyFlowOutput(
        query_summary=MediacloudQuerySummary(...),
        b2_artifact=FileUploadArtifact(...),
    )
```

### Running Flows Locally

Use the `run_flow.py` script to test flows without Prefect orchestration:

```bash
# List available flows
python run_flow.py --list

# Run a flow interactively
python run_flow.py keywords_demo --interactive

# Run with parameters
python run_flow.py keywords_demo --query "climate change" --start-date 2024-01-01 --end-date 2024-01-07
```

### Architecture

- **Flows**: Python functions decorated with `@register_flow` that define data processing pipelines
- **Tasks**: Prefect-decorated functions that perform individual operations (querying, processing, exporting)
- **Flow Registry**: Automatic discovery of flows via decorator registration

### Article deduplication

Sous-Chef includes a reusable article-deduplication helper for MediaCloud story
data. Deduplication happens “up front” in the MediaCloud discovery step and is
controlled by the `dedup_articles` flag on `MediacloudQuery`. When enabled,
the discovery task removes duplicate stories (by normalized title, keeping the
earliest publish date) before passing articles to downstream tasks.

```python
from sous_chef.tasks.deduplication_tasks import deduplicate_articles
from sous_chef.tasks.discovery_tasks import query_online_news
from sous_chef.params.mediacloud_query import MediacloudQuery

class MyParams(MediacloudQuery, CsvExportParams):
    # Use core MediaCloud params, including:
    # dedup_articles: bool = False  (defined on MediacloudQuery)

def my_flow(params: MyParams) -> MyFlowOutput:
    articles, query_summary = query_online_news(
        query=params.query,
        collection_ids=params.collection_ids,
        source_ids=params.source_ids,
        start_date=params.start_date,
        end_date=params.end_date,
        dedup_articles=params.dedup_articles,
    )
    ...
```

The helper keeps the earliest story in each duplicate group (by `publish_date`
when available) and, when `return_stats=True`, produces a secondary DataFrame
of dropped duplicates. For convenience, `query_online_news`:

- Returns the **deduplicated** articles DataFrame when `dedup_articles=True`.
- Attaches an `ArticleDeduplicationSummary` (high-level counts/config) and a
  non-serialized `duplicates_df` (the detailed duplicates DataFrame) onto the
  `MediacloudQuerySummary` artifact so flows can optionally export or inspect
  duplicates.

For example, `full_text_download_flow` exposes an `export_dedup_stats` flag on
its params. When `dedup_articles=True` and `export_dedup_stats=True`, it uploads
`query_summary.duplicates_df` as a CSV to B2 and links the resulting
`FileUploadArtifact` from the dedup summary artifact for easy analysis.

### Package Structure

```
sous_chef/
├── flow.py              # Flow registry and decorators
├── flows/               # Flow definitions
│   └── keywords_demo_flow.py
├── tasks/               # Task library
│   ├── discovery_tasks.py
│   ├── keyword_tasks.py
│   ├── aggregator_tasks.py
│   └── export_tasks.py
└── secrets.py           # Secret management

tests/                   # Test suite
└── test_export_tasks.py
```
