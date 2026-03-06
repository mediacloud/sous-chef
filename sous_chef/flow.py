from prefect import flow as prefect_flow
from typing import Dict, Callable, Any, Optional
from pydantic import BaseModel

from .artifacts import BaseArtifact

_FLOW_REGISTRY: Dict[str, Dict[str, Any]] = {}

# Canonical flow return type: mapping of names to artifacts
FlowReturn = Dict[str, BaseArtifact]


class BaseFlowOutput(BaseModel):
    """
    Base class for all flow output models.
    
    FlowOutput models define the structure of artifacts returned by flows.
    All fields should be BaseArtifact instances. This base class provides
    type safety and enables consistent handling in the kitchen.
    
    Example:
        class MyFlowOutput(BaseFlowOutput):
            query_summary: MediacloudQuerySummary
            b2_artifact: FileUploadArtifact
    """
    pass

def register_flow(
    name: str,
    description: str = "",
    params_model: Optional[type[BaseModel]] = None,
    output_model: Optional[type[BaseModel]] = None,
    admin_only: bool = False,
    restricted_fields: Optional[Dict[str, bool]] = None,
    **flow_kwargs  # Pass through to @flow decorator
):
    """
    Combined decorator: registers flow AND applies Prefect @flow.
    
    This decorator:
    1. Applies Prefect's @flow decorator
    2. Registers the flow for API discovery
    3. Stores metadata (description, params model, output model, admin_only, restricted_fields)
    
    Usage:
        @register_flow(
            name="keywords",
            description="Extract top keywords from news articles",
            params_model=KeywordsFlowParams,
            output_model=KeywordsFlowOutput
        )
        def keywords_flow(params: KeywordsFlowParams) -> KeywordsFlowOutput:
            ...
    
    Args:
        name: Flow name
        description: Flow description
        params_model: Pydantic model for flow parameters
        output_model: Pydantic model for flow outputs (FlowOutput model with BaseArtifact fields)
        admin_only: If True, only admin users can access this flow
        restricted_fields: Dict mapping output field names to True if they should
            only be returned to users with full-text access
        **flow_kwargs: Additional kwargs passed to Prefect's @flow decorator
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
            "output_model": output_model,
            "doc": flow_func.__doc__ or description,
            "admin_only": admin_only,
            "restricted_fields": restricted_fields or {},
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

def get_flow_output_schema(name: str) -> Dict[str, Any]:
    """
    Get a useful schema for flow outputs.

    Instead of returning the raw JSON schema (which often contains only
    $ref entries), this function inspects the FlowOutput model and returns,
    for each output field, the artifact type and its flattened field schema.

    Example structure:

        {
          "query_summary": {
            "artifact_type": "mediacloud_query_summary",
            "fields": { ... MediacloudQuerySummary properties ... }
          },
          "b2_artifact": {
            "artifact_type": "file_upload",
            "fields": { ... FileUploadArtifact properties ... }
          }
        }

    Args:
        name: Flow name

    Returns:
        Dict mapping output field name -> {artifact_type, fields}
    """
    flow = _FLOW_REGISTRY.get(name)
    if not flow:
        return {}
    
    output_model = flow.get("output_model")
    if not output_model:
        return {}

    outputs: Dict[str, Any] = {}

    for field_name, field in output_model.model_fields.items():
        artifact_cls = field.annotation

        # Only handle artifact fields
        if not isinstance(artifact_cls, type) or not issubclass(artifact_cls, BaseArtifact):
            continue

        # Get artifact JSON schema
        schema = artifact_cls.model_json_schema()
        properties = schema.get("properties", {})

        # Resolve local $ref entries (e.g. enums) into concrete field schemas
        defs = schema.get("$defs") or schema.get("definitions") or {}
        for prop_name, prop_schema in list(properties.items()):
            if not isinstance(prop_schema, dict):
                continue
            ref = prop_schema.get("$ref")
            if not isinstance(ref, str):
                continue

            # Only handle local refs like "#/$defs/SomeEnum"
            if ref.startswith("#/$defs/"):
                def_name = ref.split("/")[-1]
                ref_schema = defs.get(def_name)
                if isinstance(ref_schema, dict):
                    merged = {
                        **ref_schema,
                        **{k: v for k, v in prop_schema.items() if k != "$ref"},
                    }
                    properties[prop_name] = merged

        outputs[field_name] = {
            "artifact_type": getattr(artifact_cls, "artifact_type", artifact_cls.__name__),
            "fields": properties,
        }

    return outputs


def get_flow_schema(name: str) -> Dict[str, Any]:
    """
    Get JSON schema for flow parameters.
    Returns format compatible with frontend form generation.
    
    If the parameter model inherits from base models with _component_hint,
    adds x-component metadata to fields for frontend grouping.
    """
    flow = _FLOW_REGISTRY.get(name)
    if not flow:
        return {}
    
    params_model = flow.get("params_model")
    if params_model:
        # Pydantic model -> JSON schema
        schema = params_model.model_json_schema()
        properties = schema.get("properties", {})
        
        #Resolve $ref into $defs/definitions for eanums
        defs = schema.get("$defs") or schema.get("definitions") or {}

        for field_name, field_schema in list(properties.items()):
            if not isinstance(field_schema, dict):
                continue
            ref = field_schema.get("$ref")
            if not isinstance(ref, str):
                continue

            # Only handle local refs like "#/$defs/GroqModelName"
            if ref.startswith("#/$defs/"):
                def_name = ref.split("/")[-1]
                ref_schema = defs.get(def_name)
                if isinstance(ref_schema, dict):
                    # Merge referenced schema into field schema, preserving any
                    # field-level overrides (default, description, title, etc.)
                    merged = {
                        **ref_schema,
                        **{k: v for k, v in field_schema.items() if k != "$ref"},
                    }
                    properties[field_name] = merged


        # Enhance with component hints if base models are used
        if hasattr(params_model, '__mro__'):
            # Build field-to-component mapping
            field_components = {}
            
            # Walk through MRO to find base models with _component_hint
            for base_class in params_model.__mro__:
                # Skip the model itself and BaseModel
                if (base_class is params_model or 
                    base_class is BaseModel or
                    base_class is object):
                    continue
                
                # Check if this base class has a component hint
                # Try multiple ways to access it (Pydantic v2 might handle this differently)
                component_hint = None
                if hasattr(base_class, '_component_hint'):
                    component_hint = getattr(base_class, '_component_hint', None)
                elif '_component_hint' in base_class.__dict__:
                    component_hint = base_class.__dict__['_component_hint']
                
                if component_hint and isinstance(component_hint, str):
                    # Get fields defined in this base class
                    if hasattr(base_class, 'model_fields'):
                        for field_name in base_class.model_fields.keys():
                            field_components[field_name] = component_hint
            
            # Add x-component metadata to properties
            for field_name, field_schema in properties.items():
                if field_name in field_components:
                    # Ensure field_schema is a dict (it should be from Pydantic)
                    if not isinstance(field_schema, dict):
                        properties[field_name] = field_schema = {}
                    else:
                        # Work with existing dict
                        field_schema = properties[field_name]
                    
                    # Add component hint metadata
                    field_schema['x-component'] = field_components[field_name]
        
        return properties
    
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