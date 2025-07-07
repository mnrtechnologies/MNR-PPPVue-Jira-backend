import os
import json
from bson import ObjectId
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import ClientSession, BasicAuth, ClientResponseError
# from utils.credentials import get_credentials
from src.logger import get_logger

# Load environment variables and credentials
load_dotenv()
logger = get_logger(__name__)
# creds = get_credentials()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME_ISSUES = os.getenv("COLLECTION_NAME")

# NGROK base URL and endpoint
NGROK_BASE_URL = "https://f0fc-122-181-53-248.ngrok-free.app"
WEBHOOK_ENDPOINT_PATH = "/jira-webhook"
WEBHOOK_URL = f"{NGROK_BASE_URL}{WEBHOOK_ENDPOINT_PATH}"

# Payload
payload = {
    "name": "FastAPI Jira Webhook",
    "url": WEBHOOK_URL,
    "events": [
        "jira:issue_created",
        "jira:issue_updated",
        "worklog_created",
        "worklog_updated",
        "worklog_deleted"
    ],
    "excludeBody": False
}

# Async webhook creation function
async def webhook(user_id: str,db_collection):
    # client = AsyncIOMotorClient(MONGO_URI)
    try:
        # db = db_client[DATABASE_NAME]
        # collection = db[COLLECTION_NAME]

        try:
            object_id = ObjectId(user_id)
        except Exception:
            logger.error(f"Error: '{user_id}' is not a valid ObjectId format.")
            return None

        user_document = await db_collection.find_one({"_id": object_id})
        if not user_document:
            logger.error("No user found with given ID.")
            return None

        required_fields = ["jira_email", "jira_api_key", "jira_domain"]
        for field in required_fields:
            if field not in user_document:
                logger.error(f"Missing required field: {field}")
                return None

        base_url = user_document["jira_domain"].rstrip('/')
        auth = BasicAuth(user_document['jira_email'], user_document['jira_api_key'])

        async with ClientSession(auth=auth) as session:
            async with session.post(
                f"https://{base_url}/rest/webhooks/1.0/webhook",
                json=payload,
                headers={"Content-Type": "application/json"}
            ) as response:
                if response.status in [200, 201]:
                    data = await response.json()
                    logger.info("Webhook created successfully.")
                    print("webhook sucefully")
                    return data
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to create webhook: {response.status} - {error_text}")
                    return None

    except ClientResponseError as e:
        logger.error(f"Client response error: {e.status} - {e.message}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return None
    # finally:
    #     client.close()
    #     logger.info("MongoDB connection closed.")
