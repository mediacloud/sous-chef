
from prefect.blocks.system import Secret
from prefect_aws import AwsCredentials
from prefect_github import GitHubCredentials
from prefect_email import EmailServerCredentials
from prefect_docker import DockerRegistryCredentials
from pydantic_settings import BaseSettings

#These are all loaded from values provided to the environment by content in a private repository 

class SousChefCredentials(BaseSettings):
	ACCESS_KEY_ID:str
	ACCESS_KEY_SECRET:str

	DOCKER_USERNAME:str
	DOCKER_PASSWORD:str

	GMAIL_APP_USERNAME:str
	GMAIL_APP_PASSWORD:str

	GITHUB_RO_PAT:str

	MEDIACLOUD_API_KEY:str


config = SousChefCredentials()

AwsCredentials(
	aws_access_key_id=config.ACCESS_KEY_ID,
	aws_secret_access_key=config.ACCESS_KEY_SECRET
	).save("aws-s3-credentials")


DockerRegistryCredentials(
	username=config.DOCKER_USERNAME,
	password=config.DOCKER_PASSWORD
	).save("docker-auth")

GitHubCredentials(token=config.GITHUB_RO_PAT).save("sous-chef-read-only")

EmailServerCredentials(
	username=config.GMAIL_APP_USERNAME,
	password=config.GMAIL_APP_PASSWORD).save("paige-mediacloud-email-password")

Secret(value=config.MEDIACLOUD_API_KEY).save("mediacloud-api-key")