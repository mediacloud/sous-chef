from prefect import flow as prefect_flow
from typing import Dict, Callable, Any, Optional
from pydantic import BaseModel

_FLOW_REGISTRY: Dict[str, Dict[str, Any]] = {}

def register_flow(
    name: str,
    description: str = "",
    params_model: Optional[type[BaseModel]] = None,
    **flow_kwargs  # Pass through to @flow decorator
):
    """
    Combined decorator: registers flow AND applies Prefect @flow.
    
    This decorator:
    1. Applies Prefect's @flow decorator
    2. Registers the flow for API discovery
    3. Stores metadata (description, params model)
    
    Usage:
        @register_flow(
            name="keywords",
            description="Extract top keywords from news articles",
            params_model=KeywordsFlowParams
        )
        def keywords_flow(params: KeywordsFlowParams) -> Dict[str, Any]:
            ...
    """
    def decorator(flow_func: Callable) -> Callable:
        # Apply Prefect @flow decorator first
        prefect_flow_func = prefect_flow(name=name, **flow_kwargs)(flow_func)
        
        # Then register for discovery
        _FLOW_REGISTRY[name] = {
            "name": name,
            "description": description,
            "func": prefect_flow_func,  # Store the Prefect-wrapped function
            "params_model": params_model,
            "doc": flow_func.__doc__ or description
        }
        
        return prefect_flow_func
    return decorator

def get_flow(name: str) -> Optional[Dict[str, Any]]:
    """Get flow metadata by name."""
    return _FLOW_REGISTRY.get(name)

def list_flows() -> Dict[str, Dict[str, Any]]:
    """List all registered flows with metadata."""
    return {
        name: {
            "name": flow["name"],
            "description": flow["description"] or flow["doc"]
        }
        for name, flow in _FLOW_REGISTRY.items()
    }

def get_flow_schema(name: str) -> Dict[str, Any]:
    """
    Get JSON schema for flow parameters.
    Returns format compatible with frontend form generation.
    """
    flow = _FLOW_REGISTRY.get(name)
    if not flow:
        return {}
    
    params_model = flow.get("params_model")
    if params_model:
        # Pydantic model -> JSON schema
        schema = params_model.model_json_schema()
        # Return just the properties (matches current API format)
        return schema.get("properties", {})
    
    # Fallback: extract from function signature if no Pydantic model
    import inspect
    sig = inspect.signature(flow["func"])
    schema = {}
    for param_name, param in sig.parameters.items():
        if param_name == "params":
            continue
        param_type = param.annotation
        param_default = param.default
        
        type_mapping = {
            str: "string",
            int: "integer",
            float: "number",
            bool: "boolean",
            list: "array",
            dict: "object"
        }
        
        schema[param_name] = {
            "type": type_mapping.get(param_type, "string"),
            "default": param_default if param_default != inspect.Parameter.empty else None
        }
    
    return schema