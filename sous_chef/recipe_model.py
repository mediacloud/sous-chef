from pydantic import BaseModel, create_model
from typing import Any, Dict, List, Optional, Type
import yaml
import json
from string import Template
from datetime import date

# Mapping from string types in YAML to Python types
_basic_type_map = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "date": date,
}

def parse_type(type_str: str) -> Any:
    if type_str in _basic_type_map:
        return _basic_type_map[type_str]
    if type_str.startswith("List["):
        inner = type_str[5:-1]
        return List[_basic_type_map[inner]]
    if type_str.startswith("Optional["):
        inner = type_str[9:-1]
        return Optional[parse_type(inner)]
    raise ValueError(f"Unsupported type: {type_str}")


def build_model_from_recipe(recipe_yaml: dict, model_name: str = "RecipeParams") -> Type[BaseModel]:
    param_section = recipe_yaml.get("parameters", {})
    fields = {}
    for name, spec in param_section.items():
        if isinstance(spec, dict):
            type_str = spec.get("type", "str")
            default = spec.get("default", ...)
        else:
            type_str = "str"
            default = spec
        fields[name] = (parse_type(type_str), default)
    return create_model(model_name, **fields)


def render_recipe(recipe_template_str: str, params: BaseModel) -> str:
    """
    Substitute $VARS in a YAML string using validated params.
    Lists and dicts are automatically JSON-stringified.
    """
    flat_params = {
        k: v.isoformat() if isinstance(v, date)
        else json.dumps(v) if isinstance(v, (list, dict))
        else v
        for k, v in params.dict().items()
    }    
    return Template(recipe_template_str).substitute(flat_params)


def load_recipe_file(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_recipe_template_str(path: str) -> str:
    with open(path) as f:
        return f.read()

# Example usage:
# recipe_dict = load_recipe_file("some_recipe.yaml")
# RecipeParamsModel = build_model_from_recipe(recipe_dict)
# validated_params = RecipeParamsModel(**user_input)
# rendered_yaml = render_recipe(load_recipe_template_str("some_recipe.yaml"), validated_params)