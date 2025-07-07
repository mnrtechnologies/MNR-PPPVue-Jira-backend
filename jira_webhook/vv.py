from fastapi import FastAPI, Request
import uvicorn
import json
from datetime import datetime, timezone
import asyncio
from aiohttp import BasicAuth,ClientSession,ClientConnectionError,ClientResponseError
import logging
from utils.credentials import get_credentials
from src.logger import get_logger
logger = get_logger(__name__)
import http
from botocore.exceptions import ClientError
from typing import Dict, List, Optional, Any
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
from datetime import datetime, timezone
from utils.sqs import send_issue_to_sqs
app = FastAPI()
try:
    creds=get_credentials()
    required_creds=["email","api","base_url"]
    for keys in required_creds:
        if keys not in creds or not  creds['key']:
            raise ValueError(f"Missing or empty credentials")
    auth=BasicAuth(creds["email"],creds["api"])
    base_url=creds["base_url"].rstrip('/')
    headers={
        "Accept":"application/json",
        "Content-Type":"application/json"
    }   
except (KeyError, ValueError) as e:
    logger.critical(f"Failed to load or validate credentials: {e}. Exiting.")
    auth = None
    base_url = None
except Exception as e:
    logger.critical(f"An unexpected error occurred during credential loading: {e}. Exiting.")
    auth = None
    base_url = None

async def get_details(issue_id):
    try:
        url=f"{base_url}/rest/api/3/issue/{issue_id}"
        async with ClientSession(auth=auth,headers=headers) as session:
            async with session.get(url=url,headers=headers) as response:
                response.raise_for_status()
                data = await response.json()
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
            logger.warning(f"Rate limit hit: JIRA API returned 429. Retrying with backoff.")
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

now = datetime.now()
formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
async def status_transition_log(changelog):
    """
    Finds the VERY FIRST status change in the changelog histories and returns it.
    """
    items = changelog.get("items", [])
    
    # Loop through each history record
        # Loop through each change item in that history
    for item in items:
            # Check if this item is a status change
            if item.get("field") == "status":
                # If it is, create the dictionary
                first_status_change = {
                    "created":formatted_time,
                    "fromString": item.get("fromString"),
                    "toString": item.get("toString")
                }
                # Immediately return the single record inside a list and exit the function
                return [first_status_change]
                
    # If the loops finish without finding any status change, return an empty list
    return 
# Simple logger setup (replace with your src.logger if it exists)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
@app.post("/jira-webhook")
async def handle_webhook(request: Request):
    try:
        data = await request.json()
        event = data.get("webhookEvent", "unknown")
        issue = data.get("issue", {})
        issue_key = issue.get("key")

        # Extract project key
        project_key = issue.get("fields", {}).get("project", {}).get("key")

        print(issue_key)
        print(project_key)

        # Always print the full payload for debugging (uncomment when debugging)
        print("\n--- Received Jira Webhook Payload ---")
        # print(json.dumps(data, indent=2))
        print("-------------------------------------\n")

        # fields_of_interest = ['status', 'priority', 'duedate', "originalEstimate", "remainingEstimate"]

        if event == "jira:issue_updated":
            # --- NEW LOGIC TO CHECK FOR UNWANTED CHANGES ---
            # changelog = data.get("changelog")
           
            # else:
                # If there's no changelog, it means no fields were updated, so we can ignore or process
                # For this specific requirement (stop if *any* changed field is not in list),
                # if no fields changed, there's nothing to check, so we can proceed or ignore.
                # I'll let it proceed to allow printing issue details even if no relevant changelog.
                # pass
            # --- END NEW LOGIC ---

            # If we reach here, either no fields changed, or all changed fields are in our list.
            # worklogs=data.get("worklog",{})
            # worklog_created=worklogs.get("created","NA")
            # worklog_updated=worklogs.get("updated","NA")
            # worklog_started=worklogs.get("started","NA")
            # worklog_timespent=worklogs.get("timespent",'NA')


            issue = data.get("issue", {})
            changelog=data.get("changelog",{})
            status_transition=await status_transition_log(changelog)
            fields = issue.get("fields", {})
            issue_key = issue.get('key', 'N/A')  # FIX: Define issue_key
            summary = fields.get('summary', 'No summary')
            project_details=fields.get("project")
            project_name=project_details.get("name","NA")
            team_names=fields.get("customfield_10001",{})
            team_name=team_names.get("name",'NA') if team_names else "NA"
            status_name = fields.get("status", {}).get("name", "N/A")
            priority_name = fields.get("priority", {}).get("name", "N/A")
            assignee_info = fields.get("assignee")
            assignee_name = assignee_info.get("displayName", "Unassigned") if assignee_info else "Unassigned"
            due_date = fields.get('duedate', 'No due date')
            timetracking = fields.get("timetracking", {})
            original_estimate = timetracking.get("originalEstimate", "N/A")
            remaining_estimate = timetracking.get("remainingEstimate", "N/A")
            time_spent = timetracking.get("timeSpent", "N/A")
            labels = fields.get('labels', []) if isinstance(fields.get('labels'), list) else []
            reporter_data = fields.get('reporter')
            reporter = reporter_data.get('displayName', 'Unknown') if reporter_data else 'Unknown'
            status_data = fields.get('status', {})
            current_status = status_data.get('name', 'Unknown') if isinstance(status_data, dict) else 'Unknown'
            updated_str = fields.get("updated", None)
            worklog_entries = fields.get("worklog", {}).get("total", 0)
            updated_inactivity_days = None  # Default in case 'updated' is missing or malformed
            if updated_str:
                try:
                    # Parse Jira-style timestamp (e.g., 2025-06-18T12:20:00.000+0000)
                    updated_dt = datetime.strptime(updated_str, "%Y-%m-%dT%H:%M:%S.%f%z")
                    now_utc = datetime.now(timezone.utc)
                    delta_days = (now_utc - updated_dt).days
                    updated_inactivity_days = max(delta_days, 0)

                    logger.debug(f"Issue {issue_key} - Days since last update: {updated_inactivity_days}")
                except Exception as e:
                    logger.warning(f"Failed to parse 'updated' timestamp for issue {issue_key}: {e}")
            else:
                logger.info(f"No 'updated' timestamp available for issue {issue_key}") 
                        
                        
            data = {
                            "key": issue_key,
                            # "changelog":changelog,
                            "project_name":project_name,
                            "worklog_enterie":worklog_entries,
                            # "status_transition_logic": status_transition,
                            # "last_status_change_date":last_status_cha,
                            "team": team_name,
                            "summary": summary,
                            "assignee": assignee_name,
                            "reporter": reporter,
                            "labels": labels,
                            "original_estimate": original_estimate,
                            "remaining_estimate": remaining_estimate,
                            "time_logged": time_spent,
                            "status": current_status,
                            # "days_in_current_status": days_in_current_status,
                            "due_date": due_date,
                            "updated_str": updated_str,
                            "update_inactivity_days": updated_inactivity_days,
                            "priority": priority_name     
                                                                  }
            # "Updated": {updated_str}")
            if status_transition is not None:
              data["status_transition_logic"] = status_transition
              data["last_status_change_date"]= formatted_time
              
            # "Inactivity days": updated_inactivity_days       
            print(data)
            # success = await send_issue_to_sqs(data)

            if success:
                logger.info(f"Successfully sent {issue_key} to SQS.")
                return {"status": "success", "message": f"Issue {issue_key} update sent to SQS."}
            else:
                logger.error(f"Failed to send {issue_key} to SQS.")
                # raise HTTPException(status_code=500, detail="Failed to send message to SQS.")





        elif event == "jira:issue_created":
            issue = data.get("issue", {})
            fields = issue.get("fields", {})
            issue_key = issue.get('key', 'N/A')
            
            summary = fields.get('summary', 'No summary')
            status_transition=await status_transition_log(changelog)


            # --- FIX: Safely access nested dictionary values ---
            project_details = fields.get("project", {}) # Default to empty dict
            project_name = project_details.get("name", "NA")

            # --- FIX: Safely access custom field which might be None ---
            team_names = fields.get("customfield_10001", {}) # Default to empty dict
            team_name = team_names.get("name", "NA") if team_names else "NA"

            status_name = fields.get("status", {}).get("name", "N/A")
            priority_name = fields.get("priority", {}).get("name", "N/A")
            
            assignee_info = fields.get("assignee")
            assignee_name = assignee_info.get("displayName", "Unassigned") if assignee_info else "Unassigned"
            
            due_date = fields.get('duedate', 'No due date')
            team_names=fields.get("customfield_10001",'NA')
            team_name=team_names.get("name",'NA')
            timetracking = fields.get("timetracking", {})
            original_estimate = timetracking.get("originalEstimate", "N/A")
            remaining_estimate = timetracking.get("remainingEstimate", "N/A")
            time_spent = timetracking.get("timeSpent", "N/A")
            
            labels = fields.get('labels', []) if isinstance(fields.get('labels'), list) else []
            
            reporter_data = fields.get('reporter')
            reporter = reporter_data.get('displayName', 'Unknown') if reporter_data else 'Unknown'
            status_data = fields.get('status', {})
            current_status = status_data.get('name', 'Unknown') if isinstance(status_data, dict) else 'Unknown'
            updated_str = fields.get("updated", None)
            worklog_entries = fields.get("worklog", {}).get("total", 0)
            updated_inactivity_days = None  # Default in case 'updated' is missing or malformed
             
            if updated_str:
                try:
                    # Parse Jira-style timestamp (e.g., 2025-06-18T12:20:00.000+0000)
                    updated_dt = datetime.strptime(updated_str, "%Y-%m-%dT%H:%M:%S.%f%z")
                    now_utc = datetime.now(timezone.utc)
                    delta_days = (now_utc - updated_dt).days
                    updated_inactivity_days = max(delta_days, 0)
                    logger.debug(f"Issue {issue_key} - Days since last update: {updated_inactivity_days}")
                except Exception as e:
                    logger.warning(f"Failed to parse 'updated' timestamp for issue {issue_key}: {e}")
            else:
                logger.info(f"No 'updated' timestamp available for issue {issue_key}")
            print(f"✅ Issue Created: {issue_key} - {summary}")
            data ={
                            "key": issue_key,
                            "project_name":project_name,
                            "worklog_enterie":worklog_entries,
                            "changelog":changelog,
                            # "last_status_change_date":last_status_cha,
                            "team": team_name,
                            "summary": summary,
                            "assignee": assignee_name,
                            "reporter": reporter,
                            "labels": labels,
                            "original_estimate": original_estimate,
                            "remaining_estimate": remaining_estimate,
                            "time_logged": time_spent,
                            "status": current_status,
                            # "days_in_current_status": days_in_current_status,
                            "due_date": due_date,
                            "updated_str": updated_str,
                            "update_inactivity_days": updated_inactivity_days,
                            "priority": priority_name  }  
            
            if status_transition is not None:
              data["status_transition_logic"] = status_transition
              data["last_status_change_date"]= formatted_time

            print(data)
            success = await send_issue_to_sqs(data)

            if success:
                logger.info(f"Successfully sent {issue_key} to SQS.")
                return {"status": "success", "message": f"Issue {issue_key} update sent to SQS."}
            else:
                logger.error(f"Failed to send {issue_key} to SQS.")
                # raise HTTPException(status_code=500, detail="Failed to send message to SQS.")     


        else:
            print(f"⚠️  Ignoring event: {event}")

        return {"status": "received"}
    
    except Exception as e:
        logger.error(f"Error processing webhook: {e}", exc_info=True) # exc_info adds traceback
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)