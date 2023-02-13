from pprint import pprint
import argparse
from sous_chef import RunPipeline, recipe_loader

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_yaml", help="The YAML configuration file to load and run")
    args = parser.parse_args()
    with open(args.config_yaml, "r") as config_yaml:
        json_conf = recipe_loader.yaml_to_conf(config_yaml)
        
    if "name" not in json_conf:
        name = args.config_yaml.split(".")[0].split("/")[-1]
        json_conf["name"] = name
    
    print(f"Loaded configuration file at {args.config_yaml}, Running pipeline:")
    RunPipeline(json_conf)
