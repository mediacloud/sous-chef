from pprint import pprint
import argparse
from prefect import flow
from prefect.runtime import flow_run
from sous_chef import RunPipeline, recipe_loader
import json
import os

#Our main entrypoint. 

def generate_run_name():
    params = flow_run.parameters
    location = params["recipe_location"].replace("/", "-").replace("..", "").split(".")[0]
    return f"run-{location}"

def generate_run_name_folder():
    params = flow_run.parameters
    name = params["recipe_directory"].split("sous-chef-recipes")[-1].replace("/", "-")[:-1]
    return f"run{name}"

def RunFilesystemRecipe(recipe_location):
    with open(recipe_location, "r") as config_yaml:
        json_conf = recipe_loader.yaml_to_conf(config_yaml)
        
    if "name" not in json_conf:
        name = generate_run_name()
        json_conf["name"] = name
    
    print(f"Loaded recipe at {recipe_location}, Running pipeline:")
    RunPipeline(json_conf)


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

#Main flow entrypoint. 
@flow(flow_run_name=generate_run_name_folder)
def RunRecipeDirectory(recipe_directory:str):
    if "mixins.yaml" in os.listdir(recipe_directory):
        RunTemplatedRecipe(recipe_directory+"/recipe.yaml", recipe_directory+"/mixins.yaml")
    else:
        RunFilesystemRecipe(recipe_directory+"/recipe.yaml")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--recipe-directory", help="A directory with a recipe.yaml and perhaps a mixins.yaml file to generate runs from")
    args = parser.parse_args()
    RunRecipeDirectory(args.recipe_directory)
