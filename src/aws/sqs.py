import os
import boto3
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

# Access environment variables
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION")

# Set your queue URL (you already have this)
queue_url = 'https://sqs.eu-north-1.amazonaws.com/888823204260/sqs1'

# Initialize the client with credentials from .env
sqs = boto3.client(
    'sqs',
    region_name=aws_region,
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

# Send a message
response = sqs.send_message(
    QueueUrl=queue_url,
    MessageBody='Hello from .env secured script!'
)

print(f"Message sent! ID: {response['MessageId']}")
