
from prefect.blocks.system import Secret
from prefect_aws import AwsCredentials
from prefect_github import GitHubCredentials
from prefect_email import EmailServerCredentials
from prefect_docker import DockerRegistryCredentials
from pydantic_settings import BaseSettings, SettingsConfigDict
import click
import os 
#These are all loaded from values provided through a .env file in a private repository 
#Using .env because I don't think there's any reason to pollute the actual runtime with these-
#they configure the prefect server that the currently active profile points at. 

class SousChefCredentials(BaseSettings):

	ACCESS_KEY_ID:str
	ACCESS_KEY_SECRET:str

	DOCKER_USERNAME:str
	DOCKER_PASSWORD:str

	GMAIL_APP_USERNAME:str
	GMAIL_APP_PASSWORD:str

	GITHUB_RO_PAT:str

	MEDIACLOUD_API_KEY:str


@click.command()
@click.option("--env-file", default=".env", help="filesystem path to the .env file to setup from")
@click.option("--overwrite", default=True, help="toggle if existing prefect blocks should be overwritten")
def setup_prefect_blocks(env_file, overwrite):

	config = SousChefCredentials(_env_file=env_file)
	click.echo(f"Loaded config at {env_file}")

	current_prefect_profile = os.getenv("PREFECT_PROFILE")
	click.echo(f"Setting up sous-chef secret blocks on prefect with profile: {current_prefect_profile}")

	AwsCredentials(
		aws_access_key_id=config.ACCESS_KEY_ID,
		aws_secret_access_key=config.ACCESS_KEY_SECRET
		).save("aws-s3-credentials", overwrite=overwrite)


	DockerRegistryCredentials(
		username=config.DOCKER_USERNAME,
		password=config.DOCKER_PASSWORD
		).save("docker-auth", overwrite=overwrite)


	GitHubCredentials(token=config.GITHUB_RO_PAT
		).save("sous-chef-read-only", overwrite=overwrite)


	EmailServerCredentials(
		username=config.GMAIL_APP_USERNAME,
		password=config.GMAIL_APP_PASSWORD
		).save("email-password", overwrite=overwrite)


	Secret(value=config.MEDIACLOUD_API_KEY
		).save("mediacloud-api-key", overwrite=overwrite)

	click.echo(f"Completed Prefect secret block setup")

if __name__ == "__main__":
	setup_prefect_blocks()
