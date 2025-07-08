import asyncio
import aiohttp
import time
from bson import ObjectId
from typing import Dict, Optional, Any
from datetime import datetime, timezone
from utils.sqs import send_issue_to_sqs
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from src.ingestion.fetch_projects import fetch_all_project_details
from utils.credentials import get_credentials
from aiohttp import BasicAuth, ClientSession, ClientConnectionError, ClientResponseError, ClientTimeout
from src.logger import get_logger
import os
from dotenv import load_dotenv
# Initialize logger
logger = get_logger(__name__)
REQUESTS_PER_MINUTE = 100
MIN_TIME_BETWEEN_REQUESTS = 60 / REQUESTS_PER_MINUTE
_last_request_time: Optional[float] = None

mongo_uri=os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME_ISSUES")
# --- Configuration ---
class Config:
    MAX_RETRIES = 3
    MAX_CONCURRENT_REQUESTS = 10
    REQUEST_TIMEOUT = 30
    MAX_RESULTS_PER_REQUEST = 50
    CONNECTION_LIMIT = 100
    RATE_LIMIT_MAX_RETRIES = 10
    BASE_RETRY_DELAY = 1
    # --- AWS SQS Configuration ---
    SQS_QUEUE_URL = "https://sqs.eu-north-1.amazonaws.com/888823204260/sqs1"
    AWS_REGION = "eu-north-1" # Extracted from the SQS URL


config = Config()

# --- Global session and semaphore ---
_session: Optional[ClientSession] = None
_semaphore: Optional[asyncio.Semaphore] = None
_sqs_client = None


# --- Credential and Configuration Loading ---
# try:
#     creds = get_credentials()
#     required_creds = ["email", "api", "base_url", "aws_access_key_id", "aws_secret_access_key"]
#     for key in required_creds:
#         if key not in creds or not creds[key]:
#             raise ValueError(f"Missing or empty credential: '{key}'")

#     auth = BasicAuth(creds["email"], creds["api"])
#     base_url = creds["base_url"].rstrip('/')
#     headers = {
#         "Accept": "application/json",
#         "Content-Type": "application/json"
#     }
    
    # --- Initialize SQS Client ---
#     try:
#         _sqs_client = boto3.client(
#             'sqs',
#             region_name=config.AWS_REGION,
#             aws_access_key_id=creds["aws_access_key_id"],
#             aws_secret_access_key=creds["aws_secret_access_key"]
#         )
#         logger.info("AWS SQS client initialized successfully.")
#     except (NoCredentialsError, PartialCredentialsError) as e:
#         logger.critical(f"AWS credentials not found or incomplete: {e}. SQS integration will be disabled.")
#         _sqs_client = None
#     except Exception as e:
#         logger.critical(f"Failed to initialize AWS SQS client: {e}")
#         _sqs_client = None

# except (KeyError, ValueError) as e:
#     logger.critical(f"Failed to load or validate credentials: {e}. Exiting.")
#     auth = None
#     base_url = None
#     _sqs_client = None
# except Exception as e:
#     logger.critical(f"An unexpected error occurred during credential loading: {e}. Exiting.")
#     auth = None
#     base_url = None
#     _sqs_client = None


# --- Session Management ---
async def get_session(auth,headers) -> ClientSession:
    """Get or create a reusable aiohttp session with proper configuration."""
    global _session, _semaphore
    
    if _session is None or _session.closed:
        timeout = ClientTimeout(total=config.REQUEST_TIMEOUT)
        connector = aiohttp.TCPConnector(
            limit=config.CONNECTION_LIMIT,
            limit_per_host=20,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        
        _session = ClientSession(
            auth=auth,
            headers=headers,
            timeout=timeout,
            connector=connector,
            raise_for_status=False  # We'll handle status codes manually
        )
        
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)
    
    return _session

async def status_transition_log(changelog):
    """
    Finds the VERY FIRST status change in the changelog histories and returns it.
    """
    histories = changelog.get("histories", [])
    
    # Loop through each history record
    for history in histories:
        # Loop through each change item in that history
        for item in history.get("items", []):
            # Check if this item is a status change
            if item.get("field") == "status":
                # If it is, create the dictionary
                first_status_change = {
                    "created": history.get("created"),
                    "fromString": item.get("fromString"),
                    "toString": item.get("toString")
                }
                # Immediately return the single record inside a list and exit the function
                return [first_status_change]

    return None        

async def close_session():
    """Properly close the global session."""
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None

# --- Utility Functions ---
def convert_seconds_to_dh(seconds):
    """
    Converts estimate seconds into human-readable days/hours.
    Assumes 1 day = 8 working hours.
    """
    if not isinstance(seconds, (int, float)):
        return "Not set"  # Handle non-numeric input
    if not seconds or seconds <= 0:
        return "0h"
    days = seconds // 28800  # 1 day = 8h * 3600s/h = 28800s
    hours = (seconds % 28800) // 3600
    return f"{int(days)}d {int(hours)}h" if days or hours else "0h"

async def get_days_in_current_status(changelog, current_status):
    """
    Returns a tuple:
    (last_status_change_date as ISO string, days_in_current_status as int)
    If not found, returns (None, "Unknown")
    """
    if not isinstance(changelog, dict):
        logger.warning(f"Invalid changelog format: {type(changelog)}. Expected dict.")
        return None, "Unknown"

    try:
        histories = changelog.get("histories", [])
        if not isinstance(histories, list):
            logger.warning(f"Invalid changelog histories format: {type(histories)}. Expected list.")
            histories = []

        for history in reversed(histories):
            created_timestamp = history.get("created")
            if not created_timestamp:
                continue

            items = history.get("items", [])
            if not isinstance(items, list):
                continue

            for item in items:
                if item.get("field") == "status" and item.get("toString") == current_status:
                    try:
                        # Try parsing the date (with or without microseconds)
                        try:
                            status_change_date = datetime.strptime(created_timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")
                        except ValueError:
                            status_change_date = datetime.strptime(created_timestamp, "%Y-%m-%dT%H:%M:%S%z")

                        now = datetime.now(timezone.utc)
                        days = (now - status_change_date).days

                        # 💡 Convert datetime to string in ISO format for DB
                        status_change_date_str = status_change_date.isoformat()
                        return status_change_date_str, days

                    except Exception as e:
                        logger.warning(f"Error while parsing status change date: {e}")

    except Exception as e:
        logger.warning(f"Could not process changelog for current status '{current_status}': {e}", exc_info=True)

    return None, "Unknown"

# --- AWS SQS Interaction Function ---
# async def send_issue_to_sqs(issue_data: Dict[str, Any]) -> bool:
#     """
#     Sends a single issue data dictionary to the configured AWS SQS queue.

#     Args:
#         issue_data: A dictionary containing the processed issue data.

#     Returns:
#         True if the message was sent successfully, False otherwise.
#     """
#     if not _sqs_client:
#         logger.error("SQS client not available. Cannot send issue to queue.")
#         return False

#     try:
#         # SQS message body must be a string
#         message_body = json.dumps(issue_data)
        
#         response = _sqs_client.send_message(
#             QueueUrl=config.SQS_QUEUE_URL,
#             MessageBody=message_body
#         )
        
#         logger.info(f"Successfully sent issue {issue_data['key']} to SQS. Message ID: {response.get('MessageId')}")
#         return True
#     except ClientError as e:
#         logger.error(f"Failed to send issue {issue_data.get('key', 'Unknown Key')} to SQS: {e}")
#         return False
#     except Exception as e:
#         logger.error(f"An unexpected error occurred while sending issue {issue_data.get('key', 'Unknown Key')} to SQS: {e}")
#         return False


# --- JIRA API Interaction Functions ---
async def fetch_issues_for_project(headers,base_url,auth,project_key: str, startAt: int, maxResults: int, team_field_id: str = None) -> Dict[str, Any]:
    """
    Fetches issues for a given project key from JIRA.
    Includes robust retry logic for transient network issues and API rate limiting.
    """
    if not base_url or not auth:
        logger.error("API client not initialized due to missing credentials.")
        return {"issues": [], "total": 0}

    url = f"https://{base_url}/rest/api/3/search"
    
    # Build fields list - include team field if available
    fields_list = "summary,assignee,reporter,labels,duedate,priority,worklog,updated,timetracking,status,customfield_10020,customfield_10001"
    if team_field_id and team_field_id not in fields_list:
        fields_list += f",{team_field_id}"
    
    params = {
        "jql": f"project={project_key}",
        "startAt": startAt,
        "maxResults": maxResults,
        "expand": "changelog,worklog",
        "fields": fields_list
    }

    session = await get_session(auth,headers)
    
    # Use semaphore to limit concurrent requests
    async with _semaphore:
        attempt = 0
        rate_limit_attempts = 0
        
        while attempt < config.MAX_RETRIES:
            try:
                start_time = time.time()
                global _last_request_time
                now = asyncio.get_event_loop().time()

                if _last_request_time is not None:
                    sleep_time = MIN_TIME_BETWEEN_REQUESTS - (now - _last_request_time)
                    if sleep_time > 0:
                        logger.debug(f"Throttling: sleeping {sleep_time:.2f}s before making request to {project_key}")
                        await asyncio.sleep(sleep_time)

                _last_request_time = asyncio.get_event_loop().time()
                
                async with session.get(url, params=params) as response:
                    request_duration = time.time() - start_time
                    logger.debug(f"Request to {project_key} took {request_duration:.2f}s")
                    
                    # Handle rate limiting
                    if response.status == 429:
                        rate_limit_attempts += 1
                        if rate_limit_attempts > config.RATE_LIMIT_MAX_RETRIES:
                            logger.error(f"Exceeded maximum rate limit retries ({config.RATE_LIMIT_MAX_RETRIES}) for project {project_key}")
                            break
                            
                        retry_after = int(response.headers.get("Retry-After", "5"))
                        logger.warning(
                            f"Rate limited by API for project {project_key}. "
                            f"Waiting for {retry_after} seconds before retrying (attempt {rate_limit_attempts}/{config.RATE_LIMIT_MAX_RETRIES}). "
                        )
                        await asyncio.sleep(retry_after)
                        continue  # Don't increment main attempt counter for rate limits
                    
                    # Handle other HTTP errors
                    if response.status >= 500:
                        # Server-side errors - retry with backoff
                        attempt += 1
                        retry_delay = config.BASE_RETRY_DELAY * (2 ** (attempt - 1))
                        logger.warning(
                            f"Server error ({response.status}) on attempt {attempt}/{config.MAX_RETRIES} for project {project_key}. "
                            f"Retrying in {retry_delay} seconds."
                        )
                        if attempt < config.MAX_RETRIES:
                            await asyncio.sleep(retry_delay)
                            continue
                        else:
                            logger.error(f"Failed to fetch issues for {project_key} after {config.MAX_RETRIES} server error attempts.")
                            break
                    
                    elif response.status >= 400:
                        # Client errors - don't retry
                        error_text = await response.text()
                        logger.error(
                            f"Client error {response.status} for project {project_key}: {error_text}. "
                            f"This error is not retried."
                        )
                        break
                    
                    # Success case
                    if response.status == 200:
                        response_data = await response.json()
                        logger.debug(f"Successfully fetched {len(response_data.get('issues', []))} issues for project {project_key}")
                        return response_data
                    else:
                        logger.warning(f"Unexpected status code {response.status} for project {project_key}")
                        break

            except ClientConnectionError as e:
                # Handle network-related errors
                attempt += 1
                retry_delay = config.BASE_RETRY_DELAY * (2 ** (attempt - 1))
                logger.error(
                    f"Network error on attempt {attempt}/{config.MAX_RETRIES} for project {project_key}: {e}. "
                    f"Retrying in {retry_delay} seconds."
                )
                if attempt < config.MAX_RETRIES:
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Failed to fetch issues for {project_key} after {config.MAX_RETRIES} network attempts.")
                    break

            except asyncio.TimeoutError:
                # Handle timeout errors
                attempt += 1
                retry_delay = config.BASE_RETRY_DELAY * (2 ** (attempt - 1))
                logger.error(
                    f"Timeout error on attempt {attempt}/{config.MAX_RETRIES} for project {project_key}. "
                    f"Retrying in {retry_delay} seconds."
                )
                if attempt < config.MAX_RETRIES:
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Failed to fetch issues for {project_key} after {config.MAX_RETRIES} timeout attempts.")
                    break

            except Exception as e:
                # Catch any other unexpected exceptions
                attempt += 1
                retry_delay = config.BASE_RETRY_DELAY * (2 ** (attempt - 1))
                logger.exception(
                    f"An unexpected error occurred on attempt {attempt}/{config.MAX_RETRIES} for {project_key}: {e}"
                )
                if attempt < config.MAX_RETRIES:
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Failed to fetch for {project_key} after {config.MAX_RETRIES} unexpected error attempts.")
                    break

    # Return an empty response if all retries fail
    logger.warning(f"Returning empty result for project {project_key} after all retry attempts failed")
    return {"issues": [], "total": 0}

async def get_team_field_id(auth,base_url):
    """
    Dynamically discover the team field ID by searching through all custom fields.
    """
    if not base_url or not auth:
        logger.error("Cannot fetch team field ID: API client not initialized.")
        return None
    
    try:
        fields_url = f"https://{base_url}/rest/api/3/field"
        session = await get_session(auth,base_url)
        
        async with session.get(fields_url) as response:
            if response.status == 200:
                fields_data = await response.json()
                for field in fields_data:
                    if "team" in field["name"].lower():
                        team_field_id = field["id"]
                        logger.info(f"Team Field Found: {team_field_id} → {field['name']}")
                        return team_field_id
                logger.warning("No team field found in custom fields")
                return None
            else:
                logger.error(f"Failed to fetch fields: {response.status}")
                return None
    except Exception as e:
        logger.error(f"Error fetching team field ID: {e}")
        return None

async def process_all_issues(user_id,db_collection,email):
    try:
        try:
            object_id=ObjectId(user_id)
        except Exception:
            logger.error(f"Error: '{user_id}' is not a valid ObjectId format.")
            return None
        user_document =await db_collection.find_one({"_id": object_id})
        auth=BasicAuth(user_document['jira_email'],user_document['jira_api_key'])
        base_url=user_document["jira_domain"].rstrip('/')
        headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
        }
    except Exception as e:
        print(f"An error occurred: {e}")
        return None    
            
    if not base_url or not auth:
        logger.critical("Cannot process issues: API client not initialized.")
        return 0, 0 # issues_processed, issues_failed_to_send

    # if not _sqs_client:
    #     logger.critical("SQS client is not initialized. Cannot send issues to SQS.")
    #     return 0, 0

    try:
        team_field_id = await get_team_field_id(auth,base_url)
        if not team_field_id:
           logger.warning("Team field ID not found. Team information will not be available.")
           team_field_id = None
        
        # --- FIX: Call the correct function that returns project details ---
        projects_details = await fetch_all_project_details(user_id,db_collection)
        
        if not projects_details:
            logger.info("No projects found to process.")
            return 0, 0
        
        logger.info(f"Found {len(projects_details)} projects to process.")
        total_processed_and_sent = 0
        total_failed_to_send = 0

        # --- FIX: Iterate correctly through the list of dictionaries ---
        for project_detail in projects_details:
            # --- FIX: Extract key and name from the dictionary ---
            project_key = project_detail.get("key")
            project_name = project_detail.get("name", "Unknown Project Name")

            if not project_key:
                logger.warning(f"Skipping a project with no key: {project_detail}")
                continue

            startAt = 0
            maxResults = config.MAX_RESULTS_PER_REQUEST
            total_issues_for_project = -1
            project_issues_processed = 0
            
            logger.info(f"Processing project: '{project_key}' ({project_name})")

            while True:
                try:
                    # --- FIX: Pass the STRING project_key, not the whole dictionary ---
                    issues_data = await fetch_issues_for_project(headers,base_url,auth,project_key, startAt, maxResults, team_field_id)

                    issues = issues_data.get("issues", [])
                    total_for_project = issues_data.get("total", 0)

                    if not issues:
                        if startAt == 0:
                            logger.info(f"No issues found in project {project_key}.")
                        break # Exit loop for this project

                    for issue in issues:
                        # Your existing logic for processing each issue field is good.
                        # It has been kept the same.
                        issue_key = issue.get('key', 'UNKNOWN_KEY')
                        fields = issue.get('fields')
                        if not fields:
                            logger.warning(f"Issue {issue_key} has no 'fields' data. Skipping.")
                            continue
                        
                        summary = fields.get('summary', 'No summary')
                        team_name="No team assigned"
                        if team_field_id:
                            team_info = fields.get(team_field_id)
                            logger.debug(f"Issue {issue_key} - Team field ID: {team_field_id}, Team field data: {team_info}")
                            if isinstance(team_info, dict):
                                team_name = team_info.get("name", "Unnamed team")
                            elif isinstance(team_info, list) and team_info:
                                # If multiple teams can be assigned, take the first one
                                first_team = team_info[0]
                                if isinstance(first_team, dict):
                                    team_name = first_team.get("name", "Unnamed team in list")
                                else:  # Fallback for unexpected list content
                                    team_name = str(first_team)
                            elif isinstance(team_info, str) and team_info.strip():
                                team_name = team_info
                            elif team_info is None:
                                team_name = "No team assigned"
                            else:
                                logger.debug(f"Unexpected team field format for {issue_key}: {type(team_info)} - {team_info}")
                        else:
                            logger.debug(f"No team field ID available for issue {issue_key}")

                        assignee_data = fields.get('assignee', {})

                        assignee = assignee_data.get('displayName', 'Unassigned') if isinstance(assignee_data, dict) else 'Unassigned'
                        worklog_entries=fields.get("worklog",{}).get("total",0)
                        reporter_data = fields.get('reporter', {})
                        reporter = reporter_data.get('displayName', 'Unknown') if isinstance(reporter_data, dict) else 'Unknown'

                        labels = fields.get('labels', []) if isinstance(fields.get('labels'), list) else []

                        timetracking = fields.get('timetracking', {})
                        if not isinstance(timetracking, dict):
                            timetracking = {}
                            logger.warning(f"Timetracking data for {issue_key} is not a dictionary. Defaulting to empty.")

                        logger.debug(f"Issue {issue_key} timetracking data: {timetracking}")

                        original_estimate = timetracking.get('originalEstimate')
                        remaining_estimate = timetracking.get('remainingEstimate')
                        time_spent = timetracking.get('timeSpent', '0h')

                        status_data = fields.get('status', {})
                        current_status = status_data.get('name', 'Unknown') if isinstance(status_data, dict) else 'Unknown'
                        
                        updated_str = fields.get("updated", None)
                        updated_inactivity_days = None  # Default in case 'updated' is missing or malformed

                        if updated_str:
                            try:
                                # Parse Jira-style timestamp (e.g., 2025-06-18T12:20:00.000+0000)
                                updated_dt = datetime.strptime(updated_str, "%Y-%m-%dT%H:%M:%S.%f%z")
                                now_utc = datetime.now(timezone.utc)
                                delta_days=(now_utc-updated_dt).days
                                updated_inactivity_days = max(delta_days,0)
                                logger.debug(f"Issue {issue_key} - Days since last update: {updated_inactivity_days}")
                            except Exception as e:
                                logger.warning(f"Failed to parse 'updated' timestamp for issue {issue_key}: {e}")
                        else:
                            logger.info(f"No 'updated' timestamp available for issue {issue_key}")

                        changelog = issue.get("changelog", {})
                        status_transition=await status_transition_log(changelog)
                        last_status_change_date,days_in_current_status = await get_days_in_current_status(changelog, current_status)

                        due_date = fields.get('duedate', 'No due date')
                        priority_data = fields.get('priority', {})
                        priority = priority_data.get('name', 'No priority') if isinstance(priority_data, dict) else 'No priority'

                        data = {
                            "key": issue_key,
                            # "status_transition":status_transition,
                            "project_name":project_name,
                            "worklog_enterie":worklog_entries,
                            "last_status_change_date":last_status_change_date,
                            "team": team_name,
                            "summary": summary,
                            "assignee": assignee,
                            "reporter": reporter,
                            "labels": labels,
                            "original_estimate": original_estimate,
                            "remaining_estimate": remaining_estimate,
                            "time_logged": time_spent,
                            "status": current_status,
                            "days_in_current_status": days_in_current_status,
                            "due_date": due_date,
                            "updated_str": updated_str,
                            "update_inactivity_days": updated_inactivity_days,
                            "priority": priority,
                            "user_id":user_id
                        }
                        
                        if status_transition is not None:
                            data["status_transition_log"] = status_transition
                        # --- MODIFICATION: Send data to SQS instead of appending to a list ---
                        # print(data)
                        success = await send_issue_to_sqs(data,email)
                        if success:
                            total_processed_and_sent += 1
                        else:
                            total_failed_to_send += 1
                        
                        project_issues_processed += 1
                        
                    startAt += len(issues)
                    if startAt >= total_for_project:
                        logger.info(f"Finished fetching all {total_for_project} issues for project {project_key}.")
                        break

                except Exception as e:
                    logger.exception(f"An unhandled error occurred while processing issues for project {project_key}. Halting project.")
                    break # Stop processing this project on error
            
            logger.info(f"Processed {project_issues_processed} issues for project {project_key}")

        logger.info(f"Finished processing all projects.")
        return total_processed_and_sent, total_failed_to_send

    except Exception as e:
        logger.critical(f"Critical error in process_all_issues: {e}", exc_info=True)
        return 0, 0
    
    finally:
        await close_session()

# --- Main execution block ---
# --- FIX: Changed "_main_" to the correct "__main__" ---
# if __name__ == "__main__":
#     if auth and base_url:
#         try:
#             total_sent, total_failed = asyncio.run(process_all_issues())
            
#             # --- FINAL SQS PUSH STATUS REPORT ---
#             print("\n" + "="*50)
#             print("--- SCRIPT FINISHED: SQS PUSH SUMMARY ---")
#             print(f"✅ Successfully able to push {total_sent} issues to SQS.")
#             if total_failed > 0:
#                 print(f" Failed to push {total_failed} issues to SQS. Please check logs for details.")
#             else:
#                 print(" All processed issues were pushed successfully.")
#             print("="*50 + "\n")

#         except KeyboardInterrupt:
#             logger.info("Process interrupted by user.")
#         except Exception as e:
#             logger.critical(f"An error occurred during the main execution: {e}", exc_info=True)
#         finally:
#             # Ensure cleanup on exit
#             if _session and not _session.closed:
#                 loop = asyncio.get_event_loop()
#                 if loop.is_running():
#                     loop.create_task(close_session())
#                 else:
#                     asyncio.run(close_session())
#     else:
#         logger.critical("Skipping execution because Jira or AWS SQS credentials failed to load.")