import os
import asyncio
import http
from contextlib import asynccontextmanager
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, status, Request, BackgroundTasks
from pydantic import BaseModel, EmailStr, Field
from pydantic_settings import BaseSettings
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError

from aiohttp import BasicAuth, ClientSession, ClientConnectionError

# Assuming these are your custom modules
from jira_webhook.main2 import handle_webhook
from jira_webhook.webhook_creator import webhook
from src.ingestion.aws_fetch_issue import process_all_issues
from src.ingestion.fetch_projects import fetch_all_project_details
from src.logger import get_logger

# --- 1. Centralized Configuration Management ---

# Load environment variables from .env file FIRST
load_dotenv()
logger = get_logger(__name__)

class Settings(BaseSettings):
    """
    Manages application settings using Pydantic, loading from environment variables.
    It will automatically read from a .env file if `python-dotenv` is installed.
    """
    MONGO_URI: str
    DATABASE_NAME: str
    COLLECTION_NAME: str
    COLLECTION_NAME_ISSUES: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra="ignore"

# Instantiate settings. The app will fail to start if any are missing.
try:
    settings = Settings()
    # This line is crucial for debugging. It will print the loaded settings.
    print("✅ Application settings loaded successfully:")
    print(settings.model_dump_json(indent=2))
except (ValueError, TypeError) as e:
    print(f"❌ FATAL ERROR: Invalid settings configuration. Check your .env file. Details: {e}")
    exit(1)


# --- 2. Custom Exceptions ---
class AuthenticationError(Exception):
    pass

class PermissionError(Exception):
    pass

class JiraAPIError(Exception):
    def __init__(self, status_code: int, message: str = "JIRA API error"):
        super().__init__(f"{message}: {status_code}")
        self.status_code = status_code
        self.message = message


# --- 3. Pydantic Models for Data Validation ---
class JiraCredentials(BaseModel):
    """Defines the structure for incoming Jira credentials."""
    jira_domain: str = Field(..., min_length=3, description="The user's JIRA domain (e.g., your-company.atlassian.net)")
    jira_email: EmailStr = Field(..., description="The user's email, which will be used as the unique identifier.")
    jira_api_key: str = Field(..., min_length=10, description="The user's JIRA API Key.")


# --- 4. Database and Application Lifespan Management ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages application startup and shutdown events.
    Connects to MongoDB on startup and closes the connection on shutdown.
    """
    print("\nConnecting to MongoDB...")
    try:
        # Use the MONGO_URI from the validated settings object
        app.mongodb_client = AsyncIOMotorClient(settings.MONGO_URI)
        await app.mongodb_client.admin.command('ping') # Verify connection
        
        # Use the DATABASE_NAME from the validated settings object
        app.db = app.mongodb_client[settings.DATABASE_NAME]
        
        print(f"✅ Successfully connected to MongoDB.")
        print(f"   - Using database: '{settings.DATABASE_NAME}'")

    except Exception as e:
        print(f"❌ FATAL ERROR: Could not connect to MongoDB. Details: {e}")
        exit(1)
    
    yield # The application is now running

    print("Closing MongoDB connection...")
    app.mongodb_client.close()


# --- 5. FastAPI Application Initialization ---
app = FastAPI(
    title="Jira Credential Management API",
    description="An API to store and manage Jira credentials in MongoDB.",
    version="1.2.0",
    lifespan=lifespan
)


# --- 6. API Endpoints ---
@app.post("/jira-webhook")
async def jira_webhook(request: Request):
    """Handles incoming webhooks from Jira."""
    print("Received jira webhook")
    try:
        data = await request.json()
        event = data.get("webhookEvent", "unknown")
        output = await handle_webhook(event, data)
        print(output)
        return {"status": "webhook processed"}
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error processing webhook")

@app.post("/api/jira/connect",
          status_code=status.HTTP_200_OK,
          tags=["Jira Integration"],
          summary="Save Jira Credentials and Trigger Data Sync")
async def connect_and_sync_jira(background_tasks: BackgroundTasks, credentials: JiraCredentials, request: Request):
    """
    Validates Jira credentials, saves them, and starts background tasks for data synchronization.
    """
    auth = BasicAuth(credentials.jira_email, credentials.jira_api_key)
    headers = {"Accept": "application/json"}
    base_url = f"https://{credentials.jira_domain}/rest/api/3/myself"
    db = request.app.db
    
    try:
        # Step 1: Validate credentials against Jira API
        async with ClientSession(auth=auth, headers=headers) as session:
            async with session.get(base_url) as response:
                if response.status != 200:
                    if response.status == http.HTTPStatus.UNAUTHORIZED:
                        raise AuthenticationError("Unauthorized (401): Invalid API credentials.")
                    elif response.status == http.HTTPStatus.FORBIDDEN:
                        raise PermissionError("Forbidden (403): Access denied.")
                    else:
                        error_text = await response.text()
                        logger.error(f"JIRA API error: {response.status} - {error_text}")
                        raise JiraAPIError(status_code=response.status, message=error_text)

        # Step 2: If validation is successful, save credentials to the database
        credential_document = {
            "jira_email": credentials.jira_email,
            "jira_domain": credentials.jira_domain,
            "jira_api_key": credentials.jira_api_key, # Note: Storing plaintext API keys is not recommended in production
        }
        
        # Use the collection name from the settings object
        result = await db[settings.COLLECTION_NAME].insert_one(credential_document)
        new_user_id = str(result.inserted_id)
        
        print(f"Successfully saved credentials for {credentials.jira_email} with ID: {new_user_id}")

        # Step 3: Add tasks to the background
        # Use the correct collection names from settings for background tasks
        issues_collection = db[settings.COLLECTION_NAME_ISSUES]
        
        background_tasks.add_task(process_all_issues, new_user_id, issues_collection)
        background_tasks.add_task(webhook, new_user_id, issues_collection)
        # background_tasks.add_task(fetch_all_project_details, new_user_id, issues_collection)
        
        return {
            "message": "Jira credentials validated and saved. Data synchronization will begin in the background.",
            "user_id": new_user_id,
            "email": credentials.jira_email
        }

    except ClientConnectionError as e:
        logger.error(f"Network connection error to JIRA: {e}")
        raise HTTPException(status_code=503, detail="Unable to connect to JIRA.")
    except asyncio.TimeoutError:
        logger.error("Request to JIRA timed out")
        raise HTTPException(status_code=504, detail="Request to JIRA timed out.")
    except AuthenticationError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except JiraAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except PyMongoError as e:
        logger.error(f"MongoDB operation failed for email {credentials.jira_email}. Details: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not save credentials to the database."
        )
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        raise HTTPException(status_code=500, detail="An internal server error occurred.")