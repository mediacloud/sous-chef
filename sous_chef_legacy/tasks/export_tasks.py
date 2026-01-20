import os
from datetime import date
import re
from ..flowatom import FlowAtom
from .output_tasks import OutputAtom
from .utils import lazy_import
from prefect_aws import AwsCredentials
from io import BytesIO


@FlowAtom.register("ExportToS3")
class ExportToS3(FlowAtom):
    """
    Transfers the file at file_name to an s3 bucket bucket_name
    if object_date_slug is true, will replace DATE in the object name with a datestring
    """
    
    credentials_block:str
    file_name:str
    bucket_name:str
    object_name:str
    object_date_slug:bool
    _defaults:{
        "date_slug":False
    }
    report_template = "ExportToS3"


    def task_body(self):
        aws_credentials = AwsCredentials.load(self.credentials_block)
        s3_client = aws_credentials.get_boto3_session().client("s3")
        if self.object_date_slug:
            datestring = date.today().strftime("%Y-%m-%d")
            self.object_name = self.object_name.replace("DATE", datestring)

        with open(self.file_name, "rb") as f:
            resp = s3_client.upload_fileobj(f, self.bucket_name, self.object_name)
        
        self.return_values["s3_object"] = self.object_name
        self.return_values["s3_url"] = f"https://{self.bucket_name}.s3.amazonaws.com/{self.object_name}"

    

@FlowAtom.register("CSVToS3")
class CSVToS3(OutputAtom):
    """
    Outputs a CSV which includes the given columns, and then uploads that file to S3 directly.

    """
    
    credentials_block:str
    bucket_name:str
    object_name:str
    object_date_slug:bool
    replace:bool
    _defaults:{
        "replace": True,

    }


    def task_body(self):
        aws_credentials = AwsCredentials.load(self.credentials_block)
        s3_client = aws_credentials.get_boto3_session().client("s3")

        #The s3 transfer wants a bytestream- this way we don't have to save a file locally before we transmit it. 
        csv_buffer = BytesIO()
        self.data.to_csv(csv_buffer)

        #Restore the buffer to first position so we can read it later. 
        csv_buffer.seek(0) 

        #Insert a date slug into the object name, to disambiguate daily runs
        if self.object_date_slug:
            datestring = date.today().strftime("%Y-%m-%d")
            self.object_name = self.object_name.replace("DATE", datestring)

        object_index = 0
        put_success = False
        while put_success == False:
            pre, su = self.object_name.split(".")
            put_name = f"{pre}-{object_index}.{su}"

            try:
                #Fails if put_name doesn't exist
                s3_client.get_object(Bucket=self.bucket_name, Key=put_name)
                object_index += 1

            except Exception as e:
                print(e)
                resp = s3_client.put_object(Body=csv_buffer, Bucket=self.bucket_name, Key=put_name, ContentType="text/csv")
                put_success = True

        self.return_values["columns_saved"] = list(self.data.columns.values)
        self.return_values["s3_object"] = put_name
        self.return_values["s3_url"] = f"https://{self.bucket_name}.s3.amazonaws.com/{put_name}"
        
        
        