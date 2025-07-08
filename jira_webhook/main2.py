from datetime import datetime, timezone
import logging
from src.logger import get_logger
from utils.sqs import send_issue_to_sqs
from datetime import date
today = date.today()

# Initialize logger
logger = get_logger(__name__)

# --- Custom Exception Classes ---

class AuthenticationError(Exception):
    """Raised when JIRA API authentication fails (401)."""
    pass

class PermissionError(Exception):
    """Raised when JIRA API access is denied (403)."""
    pass

class JiraAPIError(Exception):
    """Raised for general JIRA API errors (non-200, non-401, non-403)."""
    def init(self, status_code: int, message: str = "JIRA API error"):
        super().init(f"{message}: {status_code}")
        self.status_code = status_code
        self.message = message

# --- Helper Functions ---

async def status_transition_log(changelog):
    """
    Finds the VERY FIRST status change in the changelog histories and returns it.
    """
    now = datetime.now()
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
    items = changelog.get("items", [])
    
    for item in items:
        if item.get("field") == "status":
            first_status_change = {
                "created": formatted_time,
                "fromString": item.get("fromString"),
                "toString": item.get("toString")
            }
            return [first_status_change]
    return None

def _calculate_inactivity_days(updated_str, issue_key):
    """Calculates the number of days since the last update."""
    if not updated_str:
        logger.info(f"No 'updated' timestamp available for issue {issue_key}")
        return None
    try:
        updated_dt = datetime.strptime(updated_str, "%Y-%m-%dT%H:%M:%S.%f%z")
        now_utc = datetime.now(timezone.utc)
        delta_days = (now_utc - updated_dt).days
        inactivity_days = max(delta_days, 0)
        logger.debug(f"Issue {issue_key} - Days since last update: {inactivity_days}")
        return inactivity_days
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to parse 'updated' timestamp for issue {issue_key}: {e}")
        return None

def _parse_issue_data(issue, changelog,user_id):
    """Parses the issue data from the webhook payload."""
    fields = issue.get("fields", {})
    issue_key = issue.get('key', 'N/A')
    
    project_details = fields.get("project", {})
    team_names = fields.get("customfield_10001")
    assignee_info = fields.get("assignee")
    reporter_data = fields.get('reporter')
    status_data = fields.get('status', {})
    timetracking = fields.get("timetracking", {})
    
    updated_str = fields.get("updated")
    updated_inactivity_days = _calculate_inactivity_days(updated_str, issue_key)

    data = {
        "key": issue_key,
        "project_name": project_details.get("name", "NA") if project_details else "NA",
        "last_ai_interaction_day":today.isoformat(),
        "worklog_enterie": fields.get("worklog", {}).get("total", 0),
        "team": team_names.get("name", "NA") if team_names else "NA",
        "summary": fields.get('summary', 'No summary'),
        "assignee": assignee_info.get("displayName", "Unassigned") if assignee_info else "Unassigned",
        "reporter": reporter_data.get('displayName', 'Unknown') if reporter_data else 'Unknown',
        "labels": fields.get('labels', []) if isinstance(fields.get('labels'), list) else [],
        "original_estimate": timetracking.get("originalEstimate", "N/A"),
        "remaining_estimate": timetracking.get("remainingEstimate", "N/A"),
        "time_logged": timetracking.get("timeSpent", "N/A"),
        "status": status_data.get('name', 'Unknown') if isinstance(status_data, dict) else 'Unknown',
        "due_date": fields.get('duedate', 'No due date'),
        "updated_str": updated_str,
        "update_inactivity_days": updated_inactivity_days,
        "priority": fields.get("priority", {}).get("name", "N/A"),
        "user_id":user_id
    }
    
    # Add changelog for created issues, as it might be relevant
    if "changelog" in changelog:
        data["changelog"] = changelog

    return data

async def _send_data_to_sqs(data, issue_key,email):
    """Sends the processed data to SQS and handles the response."""
    success = await send_issue_to_sqs(data,email)
    if success:
        logger.info(f"Successfully sent {issue_key} to SQS.")
        return {"status": "success", "message": f"Issue {issue_key} update sent to SQS."}
    else:
        logger.error(f"Failed to send {issue_key} to SQS.")
        # In a real application, you might want to raise a more specific exception
        # For now, we'll follow the original structure and raise a generic Exception
        raise Exception("Failed to send message to SQS with status code 500.")

# --- Main Webhook Handler ---

async def handle_webhook(event, data,user_id,email):
    """
    Handles incoming JIRA webhooks for issue creation and updates.
    """
    print("The webhook has been received")
    try:
        webhook_event = data.get("webhookEvent", "unknown")
        
        if webhook_event not in ["jira:issue_updated", "jira:issue_created"]:
            print(f"⚠  Ignoring event: {webhook_event}")
            return {"status": "received", "message": f"Event '{webhook_event}' ignored."}

        issue = data.get("issue", {})
        changelog = data.get("changelog", {})
        issue_key = issue.get('key', 'N/A')

        # Parse the common issue data
        parsed_data = _parse_issue_data(issue, changelog,user_id)

        # Handle status transitions
        status_transition = await status_transition_log(changelog)
        if status_transition:
            parsed_data["status_transition_logic"] = status_transition
            parsed_data["last_status_change_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(parsed_data)
        
        # Send to SQS
        return await _send_data_to_sqs(parsed_data, issue_key,email)
        

    except Exception as e:
        logger.error(f"Error processing webhook: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}  