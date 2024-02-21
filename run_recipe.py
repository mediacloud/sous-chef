from pprint import pprint
import argparse
from prefect import flow
from prefect.runtime import flow_run
from sous_chef import RunPipeline, recipe_loader
import json

#Our main entrypoint. 

def generate_run_name():
    params = flow_run.parameters
    location = params["recipe_location"].replace("/", "-").replace("..", "").split(".")[0]
    return f"run-{location}"

def generate_run_name_folder():
    params = flow_run.parameters
    name = params["recipe_directory"].split("/")[-1]
    return f"run-{name}"

@flow(flow_run_name=generate_run_name)
def RunFilesystemRecipe(recipe_location):
    with open(recipe_location, "r") as config_yaml:
        json_conf = recipe_loader.yaml_to_conf(config_yaml)
        
    if "name" not in json_conf:
        name = generate_run_name()
        json_conf["name"] = name
    
    print(f"Loaded recipe at {recipe_location}, Running pipeline:")
    RunPipeline(json_conf)


@flow(flow_run_name=generate_run_name)
def RunTemplatedRecipe(recipe_location:str, mixin_location:str):
    with open(mixin_location, "r") as infile:
        mixins = recipe_loader.load_mixins(infile)
    
    for template_params in mixins:

        with open(recipe_location, "r") as config_yaml:
            json_conf = recipe_loader.t_yaml_to_conf(config_yaml, **template_params)

        if "name" not in json_conf:
            name = recipe_location.split(".")[0].split("/")[-1]+template_params["NAME"]
            json_conf["name"] = name

        print(f"Loaded recipe at {recipe_location} with mixin {template_params['NAME']}, Running pipeline:")
        RunPipeline(json_conf) 

@flow(flow_run_name=generate_run_name_folder)
def RunRecipeFolder(recipe_directory:str):
    with open(recipe_directory+"/mixins.yaml", "r") as infile:
        mixins = recipe_loader.load_mixins(infile)

    for template_params in mixins:

        with open(recipe_directory+"/recipe.yaml", "r") as config_yaml:
            json_conf = recipe_loader.t_yaml_to_conf(config_yaml, **template_params)

        if "name" not in json_conf:
            name = recipe_location.split(".")[0].split("/")[-1]+template_params["NAME"]
            json_conf["name"] = name

        print(f"Loaded recipe at {recipe_location} with mixin {template_params['NAME']}, Running pipeline:")
        RunPipeline(json_conf) 

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--recipe-location", help="The YAML configuration file which specifies a sous-chef recipe")
    parser.add_argument("-m", "--mixin-location", help="The YAML file which specifies parameter mixins")
    parser.add_argument("-d", "--recipe-folder", help="A directory with a recipe.yaml and perhaps a mixins.yaml file to generate runs from")
    args = parser.parse_args()
    if args.recipe_directory is not None:
        RunRecipeFolder(args.recipe_directory)
    if args.mixin_location is None:
        RunFilesystemRecipe(args.recipe_location)
    else:
        RunTemplatedRecipe(args.recipe_location, args.mixin_location)
