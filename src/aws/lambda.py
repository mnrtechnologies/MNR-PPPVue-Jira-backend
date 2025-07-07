import json
import os
from dotenv import load_dotenv

# Only useful when testing locally
load_dotenv()

# Load environment variables (optional use inside logic)
ENV_NAME = os.getenv("ENV_NAME", "development")

def lambda_handler(event, context):
    try:
        print(f"Running in {ENV_NAME} environment")
        print("Received event:", json.dumps(event))

        for record in event['Records']:
            # Each record is an SQS message
            body = json.loads(record['body'])  # parse JSON string in 'body'

            # Example: extract fields
            user_id = body.get("user_id")
            action = body.get("action")
            order_id = body.get("order_id")

            # Your core logic here
            print(f"> Processing: User={user_id}, Action={action}, Order={order_id}")

            # You can call APIs, update databases, etc. here
            # For example:
            # result = do_something(user_id, action, order_id)

        return {
            'statusCode': 200,
            'body': json.dumps('Messages processed successfully.')
        }

    except Exception as e:
        print("💥 Error in Lambda:", str(e))
        raise e  # re-raise to trigger retry if needed
