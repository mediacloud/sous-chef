import argparse
from pipeline import RunPipeline, configuration_loaders

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_yaml", help="The YAML configuration file to load and run")
    args = parser.parse_args()
    with open(args.config_yaml, "r") as config_yaml:
        json_conf = configuration_loaders.yaml_to_conf(config_yaml)
        
    print(f"Loaded configuration file at {args.config_yaml}, Running pipeline:")
    RunPipeline(json_conf)
