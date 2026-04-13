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
controlled by the `dedup_strategy` field on `MediacloudQuery`. When enabled,
the discovery task removes duplicate stories according to the selected strategy
before passing articles to downstream tasks.

Available strategies:

- `none`: no deduplication (all stories are kept)
- `title_source`: keep one story per `(title, source)` pair
- `title`: keep one story per `title` across all sources (earliest publish date wins)

```python
from sous_chef.tasks.deduplication_tasks import deduplicate_articles
from sous_chef.tasks.discovery_tasks import query_online_news
from sous_chef.params.mediacloud_query import MediacloudQuery, DedupStrategy

class MyParams(MediacloudQuery, CsvExportParams):
    # Use core MediaCloud params, including:
    # dedup_strategy: DedupStrategy = DedupStrategy.none

def my_flow(params: MyParams) -> MyFlowOutput:
    articles, query_summary = query_online_news(
        query=params.query,
        collection_ids=params.collection_ids,
        source_ids=params.source_ids,
        start_date=params.start_date,
        end_date=params.end_date,
        dedup_strategy=params.dedup_strategy,
    )
    ...
```

The helper keeps the earliest story in each duplicate group when deduplication
is enabled and attaches an `ArticleDeduplicationSummary` (high-level counts and
configuration) to the `MediacloudQuerySummary` artifact so flows can see how
many stories were removed.

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

### Release

Depended on downstream by sous-chef-kitchen- versioning is managed by github tags. 

So, after merging a feature to main, bump the version number in pyproject.toml and tag the commit correspondingly.

Then to actually deploy the change to (sous-chef-kitchen)[https://github.com/mediacloud/sous-chef-kitchen], bump the version tag on line 57 of that repository's pyproject.toml, run `make requirements`, and commit.  