from pydantic import BaseModel, create_model
from typing import Any, Dict, List, Optional, Type
import yaml
import json
from string import Template
from datetime import date
from .constants import DATASTRATEGY, STEPS, RUNNAME

DATASTRATEGY_DEFAULT = {
    "id": "PandasStrategy",
    "data_location": "data/"
}

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


def finalize_recipe_config(rendered_yaml: str, name: str) -> dict:
    """
    Finalize a rendered YAML recipe by:
    - Ensuring each step has an "id" key matching its dict key
    - Inserting an empty "params" dict if missing
    - Setting a default "dataStrategy" if not present
    """
    yaml_conf = yaml.safe_load(rendered_yaml)

    steps = yaml_conf.get("steps", [])
    finalized_steps = []
    for step in steps:
        if not isinstance(step, dict) or len(step) != 1:
            raise ValueError(f"Invalid step format: {step}")
        step_id, step_conf = list(step.items())[0]
        step_conf["id"] = step_id
        step_conf.setdefault("params", {})
        finalized_steps.append(step_conf)
    
    parameters = yaml_conf.get("parameters", [])
    
    yaml_conf[RUNNAME] = name
    yaml_conf[STEPS] = finalized_steps
    yaml_conf.setdefault(DATASTRATEGY, DATASTRATEGY_DEFAULT)

    return yaml_conf

def load_recipe_file(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_recipe_template_str(path: str) -> str:
    with open(path) as f:
        return f.read()


class SousChefRecipe:
    def __init__(self, path: str, params: dict):
        if "NAME" in params:
            self.name = params["NAME"]
        else:
            self.name = "unnamed"

        self.path = path
        self.template_str = load_recipe_template_str(path)
        self.recipe_yaml = load_recipe_file(path)
        self.ParamModel = build_model_from_recipe(self.recipe_yaml)
        self.params = self.ParamModel(**params)
        self.rendered = render_recipe(self.template_str, self.params)
        self.final_config = finalize_recipe_config(self.rendered, self.name)

    def get_config(self) -> dict:
        return self.final_config

    def get_params(self) -> dict:
        return self.params.dict()

    @classmethod
    def get_param_schema(cls, path: str) -> dict:
        recipe_yaml = load_recipe_file(path)
        ParamModel = build_model_from_recipe(recipe_yaml)
        return ParamModel.schema()

    @classmethod
    def get_param_model(cls, path:str) -> Type[BaseModel]:
        recipe_yaml = load_recipe_file(path)
        return build_model_from_recipe(recipe_yaml)

