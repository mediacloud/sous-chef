from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret
from sous_chef import RunPipeline, recipe_loader

flow()
def RunRecipe(config_location):
    with open(config_location, "r") as config_yaml:
        json_conf = recipe_loader.yaml_to_conf(config_yaml)
        
    if "name" not in json_conf:
        name = config_location.split(".")[0].split("/")[-1]
        json_conf["name"] = name
    
    print(f"Loaded configuration file at {config_location}, Running pipeline:")
    RunPipeline(json_conf)


if __name__ == "__main__":
    flow.from_source(
        source=GitRepository(
            url="https://github.com/mediacloud/sous-chef/tree/refresh",
            credentials={"access_token": Secret.load("sous-chef-pat")}
        ),
        entrypoint="demo_deployment:RunRecipe",
    ).serve()


RunRecipe("test_yaml/QueryOnlineNewsTest.yaml")
    