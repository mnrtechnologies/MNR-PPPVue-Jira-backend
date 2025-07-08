import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status, Request,BackgroundTasks,Request
from pydantic import BaseModel, EmailStr, Field
from pydantic_settings import BaseSettings
# from pymongo import MongoClient
from jira_webhook.main2 import handle_webhook
from pymongo.errors import ConnectionFailure, PyMongoError
from src.ingestion.aws_fetch_issue import process_all_issues
from src.ingestion.fetch_projects import fetch_all_project_details
from motor.motor_asyncio import AsyncIOMotorClient
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, EmailStr
from bson import ObjectId
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import core_schema
from pydantic import BaseModel
import asyncio
from jira_webhook.webhook_creator import webhook
import http
from aiohttp import BasicAuth, ClientSession, ClientConnectionError, ClientResponseError
from src.logger import get_logger
logger = get_logger(__name__)

# --- 1. Configuration Management ---
# Load configuration from environment variables (.env file)
# Create a file named .env in the same directory with these values:
# MONGO_URI="mongodb://localhost:27017/"
# JIRA_DB_NAME="jira_project_management"
# JIRA_COLLECTION_NAME="credentials"
class AuthenticationError(Exception):
    pass

class PermissionError(Exception):
    pass

class JiraAPIError(Exception):
    def init(self, status_code: int, message: str = "JIRA API error"):
        super().init(f"{message}: {status_code}")
        self.status_code = status_code
        self.message = message

class JIRAAUTH(BaseModel):
    JIRA_EMAIL: str
    JIRA_DOMAIN: str
    JIRA_API: str


class Settings(BaseSettings):
    """Manages application settings using environment variables."""
    MONGO_URI: str
    DATABASE_NAME: str 
    COLLECTION_NAME: str 
    COLLECTION_NAME_USER:str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"  # This line tells Pydantic to ignore extra variables from the .env file

# Instantiate settings
try:
    settings = Settings()
except (ValueError, TypeError) as e:
    print(f"FATAL ERROR: Invalid settings configuration. Details: {e}")
    exit(1)
class PyObjectId(ObjectId):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, _source_type: any, _handler: any
    ) -> core_schema.CoreSchema:
        return core_schema.json_or_python_schema(
            json_schema=core_schema.str_schema(),
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(ObjectId),
                    core_schema.chain_schema(
                        [
                            core_schema.str_schema(),
                            core_schema.no_info_plain_validator_function(cls.validate),
                        ]
                    ),
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda x: str(x)
            ),
        )

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)

# --- 2. Pydantic Models for Data Validation ---
class JiraCredentials(BaseModel):
    """Defines the structure and validation for incoming Jira credentials."""
    jira_domain: str = Field(..., min_length=3, description="The user's JIRA domain (e.g., your-company.atlassian.net)")
    jira_email: EmailStr = Field(..., description="The user's email, which will be used as the unique identifier.")
    jira_api_key: str = Field(..., min_length=10, description="The user's JIRA API Key.")
    user_id:PyObjectId 

# class SuccessResponse(BaseModel):
#     """Standard success response model."""
#     message: str
#     email: EmailStr


# --- 3. Database and Application Lifespan Management ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages application startup and shutdown events.
    Connects to MongoDB on startup and closes the connection on shutdown.
    """
    print("Connecting to MongoDB...")
    try:
        app.mongodb_client = AsyncIOMotorClient(settings.MONGO_URI)
        await app.mongodb_client.admin.command('ping') # Verify connection
        app.db = app.mongodb_client[settings.DATABASE_NAME]
        print("Successfully connected to MongoDB.")
    except Exception as e:
        print(f"FATAL ERROR: Could not connect to MongoDB. Details: {e}")
        # In a real-world scenario, you might have a more graceful fallback or exit.
        # For this script, we will exit if the DB is not available on startup.
        exit(1)
    
    yield # The application is now running

    print("Closing MongoDB connection...")
    app.mongodb_client.close()


# --- 4. FastAPI Application Initialization ---
app = FastAPI(
    title="Jira Credential Management API",
    description="An API to store and manage Jira credentials in MongoDB.",
    version="1.1.0",
    lifespan=lifespan
)
@app.post("/jira-webhook")
async def jira_webhook(request:Request):
    print("jira webhook setup")
    try:
        data = await request.json()
    except Exception:
        logger.error("Failed to parse incoming webhook JSON.")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload.")    
    event = data.get("webhookEvent", "unknown_event")
    user_id_str = None
    try:
        issue_self_url = data.get("issue", {}).get("self", "")
        if not issue_self_url:
            logger.warning("Webhook payload did not contain 'issue.self' URL. Cannot identify user.")
            raise HTTPException(status_code=400, detail="Cannot identify Jira instance from payload.")
        domain = issue_self_url.split('/')[2]
        db_collection = request.app.db[settings.COLLECTION_NAME]
        user_doc = await db_collection.find_one({"jira_domain": domain})
        email=user_doc['jira_email']
        

        if not user_doc:
            logger.error(f"Received webhook from unknown Jira domain: {domain}")
            return {"status": "error", "detail": "User for this Jira instance not found."}
        user_id_str = str(user_doc['_id'])
        logger.info(f"Webhook received for user: {user_doc['jira_email']} (ID: {user_id_str})")
    except (KeyError, IndexError) as e:
        logger.error(f"Could not parse Jira domain from webhook payload. Error: {e}")
        raise HTTPException(status_code=400, detail="Malformed webhook payload.")
    
    # Now that we have the user_id, call your processing function from main2.py
    await handle_webhook(event, data, user_id_str,email)

    return {"status": "webhook received and processing initiated"}


# --- 5. API Endpoint ---
@app.post("/api/jira/connect",
          status_code=status.HTTP_200_OK,
          tags=["Jira Integration"],
          summary="Save or Update Jira Credentials (Plaintext)")
async def connect_and_sync_jira(background_tasks: BackgroundTasks,credentials: JiraCredentials, request: Request):

    auth = BasicAuth(credentials.jira_email, credentials.jira_api_key)
    headers = {
        "Accept": "application/json"
    }
    base_url = f"https://{credentials.jira_domain}/rest/api/3/myself"

    db = request.app.db
    email = credentials.jira_email

    # Prepare the document for MongoDB. We use the email as the _id.
    # The API key is stored directly without encryption.
    credential_document = {
        "jira_email":credentials.jira_email,
        "jira_domain": credentials.jira_domain,
        "jira_api_key": credentials.jira_api_key,
        "userid":credentials.user_id
    }

    # Perform an "upsert" operation
    try:
       async with ClientSession(auth=auth, headers=headers) as session:
           async with session.get(base_url) as response:
               if response.status == 200:
                   db_collection = request.app.db[settings.COLLECTION_NAME]
                   result=await db[settings.COLLECTION_NAME].insert_one(credential_document)
                   new_user_id=result.inserted_id
                   update_result = await db[settings.COLLECTION_NAME_USER].update_one(
                       {"_id":credentials.user_id},
                      {"$set": {"jira_credential_id": new_user_id}},
                   )
                   if update_result.matched_count == 0:
                       print("Error: Could not find user to update.")
                   else:
                      print("Success: User record linked to credentials.")
                   user_id_str = str(new_user_id)
                #    jira_webhook(user_id_str)
                   print(user_id_str)
                   background_tasks.add_task(process_all_issues, user_id_str,db_collection,credentials.jira_email)
                #    background_tasks.add_task(fetch_all_project_details, user_id_str,db_collection)

                   background_tasks.add_task(webhook, user_id_str,db_collection)
                #    background_tasks.add_task(jira_webhook,request)

                   
                #    background_tasks.add_task(fetch_all_project_details, user_id_str,db_collection)
                   return {"message": "Issue processing started in background"}
                #    return {"message": "Connection has been made successfully",
                        #    "email":credentials.jira_email}

               if response.status == http.HTTPStatus.UNAUTHORIZED:
                    logger.error("Authentication failed: Invalid API key or credentials.")
                    raise AuthenticationError("Unauthorized (401): Invalid API credentials.")
               elif response.status == http.HTTPStatus.FORBIDDEN:
                    logger.error("Access denied: You do not have permission to access this resource.")
                    raise PermissionError("Forbidden (403): Access denied.")
               elif response.status == http.HTTPStatus.TOO_MANY_REQUESTS:
                    logger.warning("Rate limit hit: JIRA API returned 429. Retrying with backoff.")
                    raise JiraAPIError(status_code=response.status, message="Rate limit exceeded")
               else:
                    error_text = await response.text()
                    logger.error(f"JIRA API error: {response.status} - {error_text}")
                    raise JiraAPIError(status_code=response.status, message=error_text)

    except ClientConnectionError as e:
        logger.error(f"Network connection error to JIRA: {e}")
        raise HTTPException(status_code=503, detail="Unable to connect to JIRA.")

    except asyncio.TimeoutError as e:
        logger.error(f"Request to JIRA timed out: {e}")
        raise HTTPException(status_code=504, detail="Request to JIRA timed out.")

    except AuthenticationError as e:
        raise HTTPException(status_code=401, detail=str(e))

    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    
    except JiraAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")     
    except PyMongoError as e:
        print(f"ERROR: MongoDB operation failed for email {email}. Details: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not save credentials to the database. Please try again later."
        )


    print(f"Successfully saved credentials for {email}. Ready to trigger background sync.")

    return {
        "message": "Jira credentials saved successfully. Data synchronization will begin shortly.",
        "email": email
    }

# --- 6. Main execution block (for direct script running) ---
# if _name_ == "_main_":
#     print("Starting FastAPI server...")
#     # To run this script:
#     # 1. Create a .env file with your MONGO_URI.
#     # 2. Run the command in your terminal: uvicorn your_script_name:app --reload
#     uvicorn.run(app, host="0.0.0.0", port=8000)