import boto3
import os
import json # Import json to pretty-print the message body
import logging

# Basic logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# It's better to exit if credentials are not found
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION", "eu-north-1") # Provide a default region
queue_url = 'https://sqs.eu-north-1.amazonaws.com/888823204260/sqs1'

if not all([aws_access_key, aws_secret_key, aws_region]):
    logging.error("AWS credentials or region are not set as environment variables. Exiting.")
    exit()

try:
    # Create SQS client
    sqs = boto3.client(
        'sqs',
        region_name=aws_region,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    logging.info(f"Polling queue for up to 20 seconds: {queue_url}")

    # Receive messages using LONG POLLING
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,  # You can fetch up to 10 messages at a time
        WaitTimeSeconds=20,      #  <--- THIS IS THE FIX
        AttributeNames=['All']   # Optional: Get all message attributes
    )

    # Process the messages
    messages = response.get('Messages', [])
    if messages:
        logging.info(f"Successfully received {len(messages)} message(s).")
        for message in messages:
            print("-" * 40)
            logging.info(f"Processing Message ID: {message['MessageId']}")
            
            # Try to parse the body as JSON for nice printing
            try:
                message_body_dict = json.loads(message['Body'])
                pretty_body = json.dumps(message_body_dict, indent=4)
                print("Message Body (parsed JSON):\n", pretty_body)
            except json.JSONDecodeError:
                print("Message Body (raw):\n", message['Body'])

            # Delete the message from the queue to prevent it from being re-processed
            receipt_handle = message['ReceiptHandle']
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            logging.info(f"Message {message['MessageId']} deleted from queue.")
            print("-" * 40)
            
    else:
        logging.info("No messages were available in the queue during the polling window.")

except Exception as e:
    logging.error(f"An error occurred: {e}", exc_info=True)