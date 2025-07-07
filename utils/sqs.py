import asyncio
import aiohttp
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
import json
import boto3
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from utils.credentials import get_credentials
from src.logger import get_logger

# Initialize logger
logger = get_logger(__name__)
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
AWS_REGION = os.getenv("AWS_REGIONS")

try:
        creds = get_credentials()
        _sqs_client = boto3.client(
            'sqs',
            region_name="eu-north-1",
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"]
        )
        logger.info("AWS SQS client initialized successfully.")
except (NoCredentialsError, PartialCredentialsError) as e:
        logger.critical(f"AWS credentials not found or incomplete: {e}. SQS integration will be disabled.")
        _sqs_client = None
except Exception as e:
        logger.critical(f"Failed to initialize AWS SQS client: {e}")
        _sqs_client = None

except (KeyError, ValueError) as e:
    logger.critical(f"Failed to load or validate credentials: {e}. Exiting.")
    _sqs_client = None
except Exception as e:
    logger.critical(f"An unexpected error occurred during credential loading: {e}. Exiting.")
    _sqs_client = None

async def send_issue_to_sqs(issue_data: Dict[str, Any]) -> bool:
    """
    Sends a single issue data dictionary to the configured AWS SQS queue.

    Args:
        issue_data: A dictionary containing the processed issue data.

    Returns:
        True if the message was sent successfully, False otherwise.
    """
    if not _sqs_client:
        logger.error("SQS client not available. Cannot send issue to queue.")
        return False

    try:
        # SQS message body must be a string
        message_body = json.dumps(issue_data)
        
        response = _sqs_client.send_message(
            QueueUrl="https://sqs.eu-north-1.amazonaws.com/888823204260/sqs1",
            MessageBody=message_body
        )
        
        # logger.info(f"Successfully sent issue {issue_data['key']} to SQS. Message ID: {response.get('MessageId')}")
        return True
    except ClientError as e:
        # logger.error(f"Failed to send issue {issue_data.get('key', 'Unknown Key')} to SQS: {e}")
        return False
    except Exception as e:
        # logger.error(f"An unexpected error occurred while sending issue {issue_data.get('key', 'Unknown Key')} to SQS: {e}")
        return False    
