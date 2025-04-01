import argparse
import json
import yaml
import traceback
from pathlib import Path
from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from prefect_aws import AwsCredentials, S3Bucket

from sous_chef import RunPipeline, SousChefRecipe
from email_flows import send_run_summary_email


def generate_run_name_folder():
    params = flow_run.parameters
    name = Path(params["recipe_dir_path"]).name.replace("/","-")
    return name


def _load_and_run_recipe(recipe_path: str, param_sets: list[dict], source_label: str = ""):
    logger = get_run_logger()
    logger.info(f"Param schema for {source_label}: {SousChefRecipe.get_param_schema(recipe_path)}")
    for params in param_sets:
        try:
            recipe = SousChefRecipe(recipe_path, params)
            run_data = RunPipeline(recipe)
            logger.info(f"Successfully ran recipe {recipe['NAME']} from {source_label}")
            return run_data

        except Exception as e:
            logger.error(f"Failed to run recipe {source_label} with params {params}: {e}")
            logger.error(traceback.format_exc())


@flow(flow_run_name=generate_run_name_folder)
def run_recipe(recipe_path: str, params: dict, email_to=["paige@mediacloud.org"]):
    run_data = _load_and_run_recipe(recipe_path, [params])
    if run_data:
        send_run_summary_email(run_data, email_to)


@flow(flow_run_name=generate_run_name_folder)
def run_s3_recipe(recipe_dir_path: str, bucket_name: str, aws_credentials_block: str, base_params: dict, email_to=["paige@mediacloud.org"]):
    logger = get_run_logger()
    aws_credentials = AwsCredentials.load(aws_credentials_block)
    #s3 = aws_credentials.get_boto3_session().client("s3")
    s3_bucket = S3Bucket(bucket_name=bucket_name, credentials=aws_credentials)

    recipe_key = f"{recipe_dir_path}/recipe.yaml"
    mixins_key = f"{recipe_dir_path}/mixins.yaml"

    try:
        mixins_data = s3_bucket.read_path(mixins_key).decode("utf-8")
        mixins_values = yaml.safe_load(mixins_data)
    except Exception as e:
        logger.error(f"Could not load mixins.yaml from S3 at {mixins_key}: {e}")
        return

    local_recipe_path = f"/tmp/{Path(recipe_key).name}"
    try:
        recipe_data = s3_bucket.read_path(recipe_key).decode("utf-8")
        with open(local_recipe_path, 'w') as f:
            f.write(recipe_data)
    except Exception as e:
        logger.error(f"Could not load recipe.yaml from S3 at {recipe_key}: {e}")
        return

    param_sets = [
        {**base_params, **params, "NAME":name}
        for mixin in mixins_values
        for name, params in mixin.items()
    ]
    run_data = _load_and_run_recipe(local_recipe_path, param_sets, source_label=f"(s3 {mixins_key})")
    if run_data:
        send_run_summary_email(run_data, email_to)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a sous-chef recipe.")
    parser.add_argument("recipe_path", type=str, help="Path to the recipe YAML file.")
    parser.add_argument("--params", type=str, help="JSON string of parameters to pass to the recipe.")
    args = parser.parse_args()

    params = json.loads(args.params) if args.params else {}
    run_recipe(args.recipe_path, params)
