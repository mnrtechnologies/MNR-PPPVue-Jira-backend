import os
from dotenv import load_dotenv
load_dotenv()

def get_credentials():
    return {
         "email":os.getenv('JIRA_EMAIL'),
         "base_url":os.getenv("JIRA_DOMAIN"),
         "api":os.getenv("JIRA_API_KEY"),
         "aws_access_key_id":os.getenv("AWS_ACCESS_KEY_ID"),
         "aws_secret_access_key":os.getenv("AWS_SECRET_ACCESS_KEY")
    }