import os
import boto3
from ..flowatom import FlowAtom
from .utils import lazy_import
from prefect_aws import AwsCredentials

@FlowAtom.register("ExportToS3")
class ExportToS3(FlowAtom):
    """
    Transfers the file at file_name to an s3 bucker bucket_name
    """
    
    credentials_block:str
    file_name:str
    bucket_name:str
    object_name:str

    def task_body(self):
        aws_credentials = AwsCredentials.load(self.credentials_block)
        s3_client = aws_credentials.get_boto3_session().client("s3")

        with open(self.file_name, "rb") as f:
            s3_client.upload_fileobj(f, self.bucket_name, self.object_name)
