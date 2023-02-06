import yaml
from .constants import PARAMS, STEPS, DATASTRATEGY
from pprint import pprint

DATASTRAT_DEFAULT = {
    "id": "PandasStrategy",
    "data_location": "data/"
}


def yaml_to_conf(yaml_stream):
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