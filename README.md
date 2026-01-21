# Sous-Chef

Sous-Chef is a data pipeline tool for building and executing data processing workflows with MediaCloud and other data sources.

## Version 3.0.0-alpha (Current)

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

### Legacy Version (v2.x)

The previous YAML-based recipe system is available in `sous_chef_legacy/` for reference but is not actively maintained. Legacy recipes used YAML configuration files and the FlowAtom/DataStrategy architecture.

### Version History**v3.0.0-alpha** - Python flow architecture with Pydantic models and Prefect integration  
**v2.3.1** - YAML recipe system with FlowAtoms and DataStrategy  
**v2.0** - Introduced SousChefRecipe class for better typing around inputs  
**v1.0** - Upgrade to Prefect 3  
**v0.1** - First beta tag for versioning
