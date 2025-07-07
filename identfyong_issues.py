from fastapi import FastAPI, Request
import uvicorn
import json
from datetime import datetime, timezone
import logging

app = FastAPI()

# Simple logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.post("/jira-webhook")
async def handle_webhook(request: Request):
    try:
        data = await request.json()
        event = data.get("webhookEvent", "unknown")

        # Debugging: Print the received payload
        print("\n--- Received Jira Webhook Payload ---")
        # print(json.dumps(data, indent=2)) # Uncomment for full payload
        print("-------------------------------------\n")

        # REMOVED: The 'fields_of_interest' list is no longer needed.

        if event == "jira:issue_updated":
            changelog = data.get("changelog")

            # REMOVED: The logic to check for unwanted changes and abort has been removed.

            # If we reach here, we process the update.
            issue = data.get("issue", {})
            fields = issue.get("fields", {})
            issue_key = issue.get('key', 'N/A')
            summary = fields.get('summary', 'No summary')
            
            project_details = fields.get("project", {})
            project_name = project_details.get("name", "NA")

            team_names = fields.get("customfield_10001", {})
            team_name = team_names.get("name", "NA") if team_names else "NA"
            
            status_name = fields.get("status", {}).get("name", "N/A")
            priority_name = fields.get("priority", {}).get("name", "N/A")
            
            assignee_info = fields.get("assignee")
            assignee_name = assignee_info.get("displayName", "Unassigned") if assignee_info else "Unassigned"
            
            due_date = fields.get('duedate', 'No due date')
            timetracking = fields.get("timetracking", {})
            original_estimate = timetracking.get("originalEstimate", "N/A")
            remaining_estimate = timetracking.get("remainingEstimate", "N/A")
            time_spent = timetracking.get("timeSpent", "N/A")
            
            labels = fields.get('labels', [])
            
            reporter_data = fields.get('reporter')
            reporter = reporter_data.get('displayName', 'Unknown') if reporter_data else 'Unknown'
            
            current_status = fields.get('status', {}).get('name', 'Unknown')
            worklog_entries = fields.get("worklog", {}).get("total", 0)
            
            print(f"📌 Issue Updated: {issue_key} - {summary}")
            print(f"   Project Name: {project_name}")
            print(f"   Team Name: {team_name}")
            print(f"   Status: {status_name}")
            print(f"   Priority: {priority_name}")
            print(f"   Assignee: {assignee_name}")
            print(f"   Due Date: {due_date}")
            print(f"   Original Estimate: {original_estimate}")
            print(f"   Remaining Estimate: {remaining_estimate}")
            print(f"   Time Spent: {time_spent}")
            print(f"   Reporter: {reporter}")
            print(f"   Labels: {labels}")

            # MODIFIED: This section now prints ALL field changes from the changelog.
            if changelog and changelog.get("items"):
                print("\n--- Field Changes ---")
                for item in changelog.get("items", []):
                    field_changed = item.get("field")
                    from_value = item.get("fromString", "N/A")
                    to_value = item.get("toString", "N/A")
                    print(f"   - Field '{field_changed}' changed from '{from_value}' to '{to_value}'")
                print("---------------------\n")


        elif event == "jira:issue_created":
            issue = data.get("issue", {})
            fields = issue.get("fields", {})
            issue_key = issue.get('key', 'N/A')
            summary = fields.get('summary', 'No summary')

            project_details = fields.get("project", {})
            project_name = project_details.get("name", "NA")

            # CORRECTED: Ensured custom field access is safe and not duplicated.
            team_info = fields.get("customfield_10001", {})
            team_name = team_info.get("name", "NA") if team_info else "NA"

            status_name = fields.get("status", {}).get("name", "N/A")
            priority_name = fields.get("priority", {}).get("name", "N/A")
            
            assignee_info = fields.get("assignee")
            assignee_name = assignee_info.get("displayName", "Unassigned") if assignee_info else "Unassigned"
            
            due_date = fields.get('duedate', 'No due date')
            timetracking = fields.get("timetracking", {})
            original_estimate = timetracking.get("originalEstimate", "N/A")
            
            labels = fields.get('labels', [])
            
            reporter_data = fields.get('reporter')
            reporter = reporter_data.get('displayName', 'Unknown') if reporter_data else 'Unknown'

            print(f"✅ Issue Created: {issue_key} - {summary}")
            print(f"   Project Name: {project_name}")
            print(f"   Team Name: {team_name}")
            print(f"   Status: {status_name}")
            print(f"   Priority: {priority_name}")
            print(f"   Assignee: {assignee_name}")
            print(f"   Due Date: {due_date}")
            print(f"   Original Estimate: {original_estimate}")
            print(f"   Reporter: {reporter}")
            print(f"   Labels: {labels}")

        else:
            print(f"⚠️  Ignoring event: {event}")

        return {"status": "received"}
    
    except Exception as e:
        logger.error(f"Error processing webhook: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)