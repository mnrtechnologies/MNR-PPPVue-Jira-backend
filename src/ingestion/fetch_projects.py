from utils.credentials import get_credentials
from aiohttp import BasicAuth, ClientSession, ClientConnectionError, ClientResponseError
from src.logger import get_logger
import asyncio
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
import backoff
import http
mongo_uri=os.getenv("MONGO_URI")
# MONGO_URI = "mongodb+srv://info:xlhQRSPwz0RwzmXD@cluster0.rmyuiop.mongodb.net/portfoliovueclient?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
logger = get_logger(__name__)


class AuthenticationError(Exception):
    """Raised when JIRA API authentication fails (401)."""
    pass


class PermissionError(Exception):
    """Raised when JIRA API access is denied (403)."""
    pass


class JiraAPIError(Exception):
    """Raised for general JIRA API errors (non-200, non-401, non-403)."""

    def __init__(self, status_code: int, message: str = "JIRA API error"):
        super().__init__(f"{message}: {status_code}")
        self.status_code = status_code
        self.message = message


# --- Initialization ---
# try:
#     creds = get_credentials()
#     required_creds = ["email", "api", "base_url"]
#     for key in required_creds:
#         if key not in creds or not creds[key]:
#             raise ValueError(f"Missing or empty credential: '{key}'")

#     auth = BasicAuth(creds["email"], creds["api"])
#     base_url = creds["base_url"].rstrip('/')

#     headers = {
#         "Accept": "application/json",
#         "Content-Type": "application/json"
#     }

# except (KeyError, ValueError) as e:
#     logger.critical(f"Failed to load or validate credentials: {e}. Exiting.")
#     auth = None
#     base_url = None
# except Exception as e:
#     logger.critical(f"An unexpected error occurred during credential loading: {e}. Exiting.")
#     auth = None
#     base_url = None


# Predicate function for backoff to check for 429 status
def is_rate_limit_error(e):
    return isinstance(e, JiraAPIError) and e.status_code == http.HTTPStatus.TOO_MANY_REQUESTS


@backoff.on_exception(
    backoff.expo,
    (ClientConnectionError, ClientResponseError, asyncio.TimeoutError, JiraAPIError),
    max_tries=5,
    factor=2,
    logger=logger,
    giveup=lambda e: not is_rate_limit_error(e) if isinstance(e, JiraAPIError) else False
)
async def fetch_projects(page_start_at: int, page_max_results: int,base_url,auth,headers) -> dict:
    """
    Fetches projects from the JIRA API with pagination.
    """

    if not base_url or not auth:
        logger.error("JIRA API client not initialized due to missing credentials.")
        raise RuntimeError("JIRA API client not initialized.")

    url = f"https://{base_url}/rest/api/3/project/search"
    params = {
        "startAt": page_start_at,
        "maxResults": page_max_results
    }

    try:
        async with ClientSession(auth=auth, headers=headers) as session:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                logger.info(
                    f"Successfully fetched projects starting at {page_start_at} "
                    f"with {len(data.get('values', []))} results."
                )
                return data

    except ClientResponseError as e:
        status = e.status
        text = e.message

        if status == http.HTTPStatus.UNAUTHORIZED:
            logger.error("Authentication failed: Invalid API key or credentials.")
            raise AuthenticationError("Unauthorized (401): Invalid API credentials.") from e
        elif status == http.HTTPStatus.FORBIDDEN:
            logger.error("Access denied: You do not have permission to access this resource.")
            raise PermissionError("Forbidden (403): Access denied.") from e
        elif status == http.HTTPStatus.TOO_MANY_REQUESTS:
            logger.warning("Rate limit hit: JIRA API returned 429. Retrying with backoff.")
            raise JiraAPIError(status_code=status, message="Rate limit exceeded") from e
        else:
            logger.error(f"JIRA API error: {status} - {text}")
            raise JiraAPIError(status_code=status, message=text) from e

    except ClientConnectionError as e:
        logger.error(f"Network connection error to JIRA: {e}")
        raise

    except asyncio.TimeoutError as e:
        logger.error(f"Request to JIRA timed out: {e}")
        raise

    except Exception as e:
        logger.exception(f"An unexpected error occurred during API call: {e}")
        raise


# --- Main Logic Function ---
async def fetch_all_project_details(user_id,db_collection) -> list[dict]:
    """
    Fetches all project details (key and name) from JIRA by handling pagination.

    Returns:
        list[dict]: A list of dictionaries, where each dictionary contains the project 'key' and 'name'.
    """
    all_project_details = []
    page_start_at = 0
    page_max_results = 50
    # try:
    #  creds = get_credentials()
    #  required_creds = ["email", "api", "base_url"]
    #  for key in required_creds:
    #     if key not in creds or not creds[key]:
    #         raise ValueError(f"Missing or empty credential: '{key}'")

    #  auth = BasicAuth(creds["email"], creds["api"])
    #  base_url = creds["base_url"].rstrip('/')

    #  headers = {
    #     "Accept": "application/json",
    #     "Content-Type": "application/json"
    # }

    # except (KeyError, ValueError) as e:
    #  logger.critical(f"Failed to load or validate credentials: {e}. Exiting.")
    #  auth = None
    #  base_url = None
    # except Exception as e:
    #   logger.critical(f"An unexpected error occurred during credential loading: {e}. Exiting.")
    #   auth = None
    #   base_url = None

    # logger.info("Starting to fetch all JIRA project details...")

    try:
        # client=AsyncIOMotorClient(mongo_uri)
        # db=client[DATABASE_NAME]
        # collection=db[COLLECTION_NAME]
        try:
            object_id = ObjectId(user_id)
        except Exception:
            print(f"Error: '{user_id}' is not a valid ObjectId format.")
            return None    
        user_document = await db_collection.find_one({"_id": object_id})
        auth=BasicAuth(user_document['jira_email'],user_document['jira_api_key'])
        base_url=user_document["jira_domain"].rstrip('/')
        headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
        }
    except Exception as e:
        logger.error(e)
        return None    
    # finally:
    #     if client:
    #         client.close()
    #         logger.info("connection to mongodb has been closed")



    if auth is None or base_url is None:
        logger.critical("Cannot fetch project details because JIRA API client was not initialized due to credential errors.")
        return []

    try:
        while True:
            projects_data = await fetch_projects(page_start_at, page_max_results,base_url=base_url,auth=auth,headers=headers)
            values = projects_data.get("values", [])
            total_projects = projects_data.get("total", 0)

            if not values:
                logger.info("No more projects found or first page was empty.")
                break

            for project in values:
                key = project.get("key")
                name = project.get("name")
                if key and name:
                    all_project_details.append({"key": key, "name": name})

            logger.info(f"Fetched {len(all_project_details)} of {total_projects} projects so far.")

            page_start_at += page_max_results
            if page_start_at >= total_projects:
                logger.info(f"Finished fetching all {len(all_project_details)} projects.")
                break

        logger.info("Successfully retrieved all project details.")
        print(all_project_details)
        return all_project_details

    except AuthenticationError:
        logger.critical("Authentication failed. Please check your JIRA API credentials.")
    except PermissionError:
        logger.critical("Permission denied. Ensure your JIRA user has necessary access rights.")
    except JiraAPIError as e:
        logger.critical(f"A JIRA API error occurred (after retries): {e.message} (Status: {e.status_code})")
    except ClientConnectionError:
        logger.critical("Failed to connect to JIRA. Please check network connectivity and JIRA server status.")
    except asyncio.TimeoutError:
        logger.critical("The request to JIRA timed out. This might indicate network issues or a slow JIRA server.")
    except Exception as e:
        logger.critical(f"An unhandled error occurred during project detail fetching: {e}", exc_info=True)

    return []


# --- Main Execution ---
if __name__ == "__main__":
    project_data = asyncio.run(fetch_all_project_details("68696f783ad17ea0c39cbc65"))
    if project_data:
        print(f"Retrieved {len(project_data)} project details:")
        for item in project_data[:5]:  # Print first 5 for brevity
            print(f"  - Key: {item['key']}, Name: {item['name']}")
    else:
        print("Failed to retrieve project details.")
