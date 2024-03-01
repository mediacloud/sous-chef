from pprint import pprint
import argparse
from prefect import flow
from prefect.runtime import flow_run
from sous_chef import RunPipeline, recipe_loader
import json
import os
from copy import copy   

from datetime import date, timedelta, datetime

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

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


#Not treating these as flows so as to limit crud in the prefect cloud ui.
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


#Take a recipe and some mixins, then run it iteratively over a sequence of timeframes. 
#To start, just a 'daily' thing.
@flow(flow_run_name=generate_run_name_folder)
def IteratedRecipe(recipe_directory:str, start_date: str, end_date: str|None = None):

    recipe_location = recipe_directory+"recipe.yaml"
    mixin_location = recipe_directory+"mixins.yaml"
    
    with open(mixin_location, "r") as infile:
        mixins = recipe_loader.load_mixins(infile)

    if end_date is None:
        end_date = datetime.today()
    else:
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

    start_date = datetime.strptime(start_date, "%Y-%m-%d")

    #Iterate over all the days in the daterange
    for window_end in daterange(start_date, end_date):
        window_start = window_end - timedelta(days=1)

        for template_params in mixins:
            template_params = copy(template_params)
            template_params["START_DATE"] = f"'{window_start.strftime("%Y-%m-%d")}'"
            template_params["END_DATE"] = f"'{window_end.strftime("%Y-%m-%d")}'"
            template_params["NAME"] += f"-{window_start.strftime("%Y-%m-%d")}"

            with open(recipe_location, "r") as config_yaml: 
                json_conf = recipe_loader.t_yaml_to_conf(config_yaml, **template_params)

            
            if "name" not in json_conf:
                name = recipe_location.split(".")[0].split("/")[-1]+template_params["NAME"]
                json_conf["name"] = name
            
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
    parser.add_argument("-s", "--start-date", help="Start date in YYYY-MM-DD to iterate the recipe query over. Triggers iterated recipe")
    parser.add_argument("-e", "--end-date", help="End date in YYYY-MM-DD to iterate the recipe query over. Triggers iterated recipe. Defaults to today if none.")
    args = parser.parse_args()
    if args.start_date is None:
        RunRecipeDirectory(args.recipe_directory)
    else:
        IteratedRecipe(args.recipe_directory, args.start_date)
