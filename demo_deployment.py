from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret

from sous_chef import Pipeline, recipe_loader

@flow()
def RunRecipe(config_location):


    with open(config_location, "r") as config_yaml:
        json_conf = recipe_loader.yaml_to_conf(config_yaml)
        
    if "name" not in json_conf:
        name = config_location.split(".")[0].split("/")[-1]
        json_conf["name"] = name
    
    print(f"Loaded configuration file at {config_location}, Running pipeline:")
    pipeline = Pipeline(json_conf)
    #pipeline.run()

reqs = """matplotlib==3.8.2, mediacloud==4.1.3, mc-providers==0.5.4, wayback-news-search==1.2.*, numpy==1.26.4, pandas==2.2.0, prefect==2.14.20, pydantic==2.6.1, pytest==8.0.0, PyYAML==6.0.1, Requests==2.31.0, transformers==4.37.2, yake==0.4.8"""



if __name__ == "__main__":

    f = flow.from_source(
        source=GitRepository(
            url="https://github.com/mediacloud/sous-chef",
            credentials={"access_token": Secret.load("sous-chef-pat")}
        ),
        entrypoint="demo_deployment.py:RunRecipe",
        
    ).deploy(
        name="sous-chef-test",
        work_pool_name="Guerin",
        cron="0 1 * * *",
        parameters={
            "config_location":"test_yaml/QueryOnlineNewsTest.yaml"
        },
        job_variables={"env": {"EXTRA_PIP_PACKAGES": "boto3"} }

    )

    



    
    