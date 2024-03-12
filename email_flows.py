from prefect import flow
from prefect_email import EmailServerCredentials, email_send_message



@flow
def send_email_flow(email_addresses):
    email_server_credentials = EmailServerCredentials.load("paige-mediacloud-email-password")
    
    for email_address in email_addresses:
    	subject = email_send_message.with_options(name=f"email {email_address}").submit(
            email_server_credentials=email_server_credentials,
            subject="Example Flow Notification using Gmail",
            msg="This proves email_send_message works!",
            email_to=email_address,
        )

    
##SOOO 
#This will need to generate a run summary somehow, and point to the outputs. 

send_email_flow(["nano3.14@gmail.com"])