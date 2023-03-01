import yaml
import copy
import re
from .constants import PARAMS, STEPS, DATASTRATEGY, VARS
from pprint import pprint

DATASTRAT_DEFAULT = {
    "id": "PandasStrategy",
    "data_location": "data/"
}


def yaml_to_conf(yaml_stream):
    ###parse a yaml file into a valid configuration json
    conf = yaml.safe_load(yaml_stream)
    clean_steps = []
    
    #So that the YAML can use the task name as the dict key, we have to 
    #do a little bit of shuffling 
    for step in conf[STEPS]:
        
        step_conf = list(step.values())[0]
        step_id = list(step.keys())[0]
        step_conf["id"] = step_id
        if PARAMS not in step_conf:
            step_conf[PARAMS] = {}
        clean_steps.append(step_conf)

    conf[STEPS] = clean_steps
    
    if DATASTRATEGY not in conf:
        conf[DATASTRATEGY] = DATASTRAT_DEFAULT
    
    return conf

"templated_yaml -> t_yaml"
def t_yaml_to_conf(yaml_stream, **kwargs):
    ###parse a templated yaml file into a 
    vars_ = set(re.findall("\$[\w\d]*", yaml_stream))
    
    pre_subbed = yaml.safe_load(yaml_stream)
   
    if VARS in pre_subbed:
        var_reference = pre_subbed[VARS]
    
    subbed_str = copy.copy(yaml_stream)
    for v in vars_:
        var_name = v[1:]
        if var_name not in kwargs:
            raise RuntimeError(f"Missing required configuration variable {var_name}: {var_reference[v]}")
        
        value = kwargs[v[1:]]
        reg = f"\\{v}"
        subbed_str = re.sub(reg, value, subbed_str)
    
    conf = yaml.safe_load(subbed_str)
    if VARS in conf:
        conf[VARS] = var_reference
    
    #So that the YAML can use the task name as the dict key, we have to 
    #do a little bit of shuffling 
    clean_steps = []
    for step in conf[STEPS]:
        
        step_conf = list(step.values())[0]
        step_id = list(step.keys())[0]
        step_conf["id"] = step_id
        if PARAMS not in step_conf:
            step_conf[PARAMS] = {}
        clean_steps.append(step_conf)

    conf[STEPS] = clean_steps
    
    if DATASTRATEGY not in conf:
        conf[DATASTRATEGY] = DATASTRAT_DEFAULT
        
    return conf