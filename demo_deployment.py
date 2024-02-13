from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret
from prefect.deployments.steps import pip_install_requirements
from sous_chef import Pipeline, recipe_loader

@flow()
def RunRecipe(config_location):
    pip_install_requirements()

    with open(config_location, "r") as config_yaml:
        json_conf = recipe_loader.yaml_to_conf(config_yaml)
        
    if "name" not in json_conf:
        name = config_location.split(".")[0].split("/")[-1]
        json_conf["name"] = name
    
    print(f"Loaded configuration file at {config_location}, Running pipeline:")
    pipeline = Pipeline(json_conf)
    #pipeline.run()



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
        }
    )

    



    
    