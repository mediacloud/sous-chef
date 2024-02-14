from pprint import pprint
import argparse
from prefect import flow
from sous_chef import RunPipeline, recipe_loader

#Our main entrypoint. 

@flow()
def RunFilesystemRecipe(config_location):
    with open(config_location, "r") as config_yaml:
        json_conf = recipe_loader.yaml_to_conf(config_yaml)
        
    if "name" not in json_conf:
        name = config_location.split(".")[0].split("/")[-1]
        json_conf["name"] = name
    
    print(f"Loaded configuration file at {config_location}, Running pipeline:")
    RunPipeline(json_conf)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_yaml", help="The YAML configuration file to load and run")
    args = parser.parse_args()
    RunFilesystemRecipe(args.config_yaml)