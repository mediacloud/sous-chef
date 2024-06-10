from pprint import pprint
import argparse
from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from sous_chef import RunPipeline, recipe_loader
import json
import os
from copy import copy   

from prefect_aws import AwsCredentials
from datetime import date, timedelta, datetime

from email_flows import send_run_summary_email


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def generate_run_name_folder():
    params = flow_run.parameters
    name = params["recipe_directory"].split("sous-chef-recipes")[-1].replace("/", "-")
    return name.strip("-")


def RunFilesystemRecipe(recipe_stream, recipe_location, test:bool):
    logger = get_run_logger()
    json_conf = recipe_loader.yaml_to_conf(recipe_stream)
        
    if "name" not in json_conf:
        name = recipe_location.replace("/", "-").replace("..", "").split(".")[0]
        json_conf["name"] = name
    

    logger.info(f"Loaded recipe at {recipe_location}, Running pipeline:")
    run_data = {json_conf["name"] : RunPipeline(json_conf)}
    return run_data


def RunTemplatedRecipe(recipe_str:str, mixin_str:str, recipe_location:str, test:bool):
    logger = get_run_logger()
    
    mixins = recipe_loader.load_mixins(mixin_str)
    run_data = {}
    for template_params in mixins:

        json_conf = recipe_loader.t_yaml_to_conf(recipe_str, **template_params)

        if "name" not in json_conf:
            name = recipe_location.split(".")[0].split("/")[-1]+template_params["NAME"]
            json_conf["name"] = name

        logger.info(f"Loaded recipe at {recipe_location} with mixin {template_params['NAME']}, Running pipeline:")
        run_data[json_conf["name"]] = RunPipeline(json_conf) 

    return run_data


#Run a query and recipe over a sequence of days
@flow(flow_run_name=generate_run_name_folder)
def IteratedRecipe(recipe_directory:str, start_date: str, end_date: str|None = None):
    logger = get_run_logger()
    recipe_location = recipe_directory+"recipe.yaml"
    mixin_location = recipe_directory+"mixins.yaml"
    
    recipe_stream = open(recipe_directory+"/recipe.yaml", "r").read()
    mixin_stream = open(recipe_directory+"/mixins.yaml", "r").read()

    run_data = RunIteratedRecipe(recipe_stream, recipe_location, mixin_stream, start_date, end_date)



#As above but loading content arbitrarily as strs instead of file locations. 
def RunIteratedRecipe(recipe_str:str, recipe_location:str, mixin_str: str, start_date:str, end_date:str,
                        email_to:list=["paige@mediacloud.org"]):
    logger = get_run_logger()
    mixins = recipe_loader.load_mixins(mixin_str)

    if end_date is None:
        end_date = datetime.today()
    else:
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

    start_date = datetime.strptime(start_date, "%Y-%m-%d")

    run_data = {}
    #Iterate over all the days in the daterange
    for window_end in daterange(start_date, end_date):
        date_run_data = {}
        window_start = window_end - timedelta(days=1)
        
        window_start = window_start.strftime("%Y-%m-%d")
        window_end = window_end.strftime("%Y-%m-%d")

        for template_params in mixins:
            template_params = copy(template_params)
            template_params["START_DATE"] = f"'{window_start}'"
            template_params["END_DATE"] = f"'{window_end}'"
            template_params["NAME"] += f"-{window_start}"

            json_conf = recipe_loader.t_yaml_to_conf(recipe_str, **template_params)

            if "name" not in json_conf:
                name = recipe_location.split(".")[0].split("/")[-1]+template_params["NAME"]
                json_conf["name"] = name

            logger.info(f"Loaded recipe at {recipe_location} with mixin {template_params['NAME']}, Running pipeline:")

            date_run_data[name] = RunPipeline(json_conf) 

        send_run_summary_email(date_run_data, email_to)
        run_data[window_end] = date_run_data
            
    return run_data 




#Main flow entrypoint. 
@flow(flow_run_name=generate_run_name_folder)
def RunRecipeDirectory(recipe_directory:str, email_to:list = ["paige@mediacloud.org"], test:bool=False):
    
    if "mixins.yaml" in os.listdir(recipe_directory):
        recipe_stream = open(recipe_directory+"/recipe.yaml", "r").read()
        mixin_stream = open(recipe_directory+"/mixins.yaml", "r")

        run_data = RunTemplatedRecipe(recipe_stream, mixin_stream, recipe_directory, test)
    else:
        recipe_stream = open(recipe_directory+"/recipe.yaml", "r").read()
        run_data = RunFilesystemRecipe(recipe_stream, recipe_directory)
    
    send_run_summary_email(run_data, email_to)
    


@flow(flow_run_name=generate_run_name_folder)
def RunS3BucketRecipe(credentials_block_name: str, recipe_bucket:str, recipe_directory:str, email_to:list = ["paige@mediacloud.org"], test:bool=False):
    ##Pull down recipe data from S3, then run that recipe in the local environment. 
    aws_credentials = AwsCredentials.load(credentials_block_name)
    s3_client = aws_credentials.get_boto3_session().client("s3")

    all_objects = s3_client.list_objects_v2(
        Bucket=recipe_bucket
        )

    objects = [o["Key"] for o in all_objects["Contents"] if recipe_directory in o["Key"] and "." in o["Key"]]
    
    order_content = {}
    for component in objects:
        final_name = component.split("/")[-1]
        order_content[final_name] = s3_client.get_object(Bucket=recipe_bucket, Key=component)["Body"].read().decode('utf-8') 

    
    if any(["mixins.yaml" in o for o in objects]):
        run_data = RunTemplatedRecipe(order_content["recipe.yaml"], order_content["mixins.yaml"], recipe_directory, test)
    else:
        run_data = RunFilesystemRecipe(order_content["recipe.yaml"], recipe_directory, test)

    send_run_summary_email(run_data, email_to)


###In progress....
@flow(flow_run_name=generate_run_name_folder)
def IteratedS3BucketRecipe(credentials_block_name:str, recipe_bucket:str, recipe_directory:str, 
                             start_date:str, end_date:str):

    aws_credentials = AwsCredentials.load(credentials_block_name)
    s3_client = aws_credentials.get_boto3_session().client("s3")

    all_objects = s3_client.list_objects_v2(
        Bucket=recipe_bucket
        )

    objects = [o["Key"] for o in all_objects["Contents"] if recipe_directory in o["Key"] and ".yaml" in o["Key"]]
    
    order_content = {}
    for component in objects:
        final_name = component.split("/")[-1]
        order_content[final_name] = s3_client.get_object(Bucket=recipe_bucket, Key=component)["Body"].read().decode('utf-8') 

    RunIteratedRecipe(order_content["recipe.yaml"], recipe_directory, order_content["mixins.yaml"], start_date, end_date)



if __name__ == "__main__":
    #These entrypoints are here for the sake of local development before deployment. 
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--recipe-directory", help="A directory with a recipe.yaml and perhaps a mixins.yaml file to generate runs from")
    parser.add_argument("-s", "--start-date", help="Start date in YYYY-MM-DD to iterate the recipe query over. Triggers iterated recipe")
    parser.add_argument("-e", "--end-date", help="End date in YYYY-MM-DD to iterate the recipe query over. Triggers iterated recipe. Defaults to today if none.")
    parser.add_argument("-b", "--bucket", action='store_true')
    parser.add_argument("-t", "--test", action='store_true', help="Validate recipe")

    args = parser.parse_args()
    if args.bucket:
        RunS3BucketRecipe("aws-s3-credentials", "sous-chef-recipes", args.recipe_directory, test=args.test)

    elif args.start_date is None:
        RunRecipeDirectory(args.recipe_directory, test=args.test)
    else:
        IteratedRecipe(args.recipe_directory, args.start_date)
