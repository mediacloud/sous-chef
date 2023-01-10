import yaml


def yaml_to_conf(yaml_stream):
    conf = yaml.safe_load(yaml_stream)
    clean_steps = []
    
    #So that the YAML can use the task name as the dict key, we have to 
    #do a little bit of shuffling 
    for step in conf["steps"]:

        step_conf = list(step.values())[0]
        step_id = list(step.keys())[0]
        step_conf["id"] = step_id

        clean_steps.append(step_conf)

    conf["steps"] = clean_steps
    return conf