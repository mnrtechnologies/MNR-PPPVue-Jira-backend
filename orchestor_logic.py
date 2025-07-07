import base64
import json
import httpx
import uvicorn
from typing import Annotated

from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel, EmailStr, StringConstraints
from dotenv import set_key, find_dotenv, load_dotenv, get_key

# Assume your issue fetching logic is in this function
from src.ingestion.fetch_issues import process_all_issues

# --- Pydantic Model for API Input ---
class JiraAuthRequest(BaseModel):
    Jira_API_Key: Annotated[str, StringConstraints(min_length=20)]
    JIRA_EMAIL: EmailStr
    JIRA_DOMAIN: Annotated[
        str, StringConstraints(pattern=r"^(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}$")
    ]

# --- FastAPI Application Instance ---
app = FastAPI()

# --- Helper Functions ---

async def validate_jira_credentials(auth: JiraAuthRequest) -> bool:
    """Validates Jira credentials by making a request to the /myself endpoint."""
    url = f"https://{auth.JIRA_DOMAIN}/rest/api/3/myself"
    try:
        auth_string = f"{auth.JIRA_EMAIL}:{auth.Jira_API_Key}".encode("utf-8")
        basic_auth = base64.b64encode(auth_string).decode("utf-8")
    except Exception:
        return False
    
    headers = {"Authorization": f"Basic {basic_auth}", "Accept": "application/json"}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
            return response.status_code == 200
    except httpx.RequestError:
        return False

def create_jira_webhook():
    """
    Reads credentials from .env and creates a Jira webhook.
    This is now a self-contained utility function.
    """
    env_file_path = find_dotenv()
    if not env_file_path:
        print(" Could not find .env file. Cannot create webhook.")
        return

    # Load credentials securely from the .env file
    api_token = get_key(env_file_path, "JIRA_API_KEY")
    jira_email = get_key(env_file_path, "JIRA_EMAIL")
    jira_domain = get_key(env_file_path, "JIRA_DOMAIN")
    ngrok_url = get_key(env_file_path, "NGROK_BASE_URL") # IMPORTANT: NGROK URL from .env

    if not all([api_token, jira_email, jira_domain, ngrok_url]):
        print(" Missing one or more required .env variables for webhook creation.")
        return

    webhook_full_url = f"{ngrok_url}/jira-webhook"
    
    payload = {
        "name": "Project Management AI Tool Webhook",
        "url": webhook_full_url,
        "events": ["jira:issue_created", "jira:issue_updated"],
        "excludeIssueDetails": False
    }

    auth = httpx.BasicAuth(jira_email, api_token)
    
    print(f"Attempting to register webhook to: {webhook_full_url}")
    try:
        with httpx.Client() as client:
            response = client.post(
                f"https://{jira_domain}/rest/webhooks/1.0/webhook",
                auth=auth,
                headers={"Content-Type": "application/json"},
                json=payload
            )
            if response.status_code == 201:
                print("✅ Webhook created successfully!")
                print(json.dumps(response.json(), indent=2))
            else:
                print(f" Failed to create webhook: {response.status_code}")
                print(f"Response: {response.text}")
    except httpx.RequestError as e:
        print(f" An error occurred during the webhook request: {e}")


async def background_sync_task(auth: JiraAuthRequest):
    """
    The main background task that handles saving credentials,
    fetching issues, and creating the webhook.
    """
    try:
        print(" Starting background task: Saving credentials...")
        env_file_path = find_dotenv()
        if not env_file_path:
             # If .env doesn't exist, python-dotenv will create it in the current directory
            with open(".env", "w") as f:
                pass
            env_file_path = find_dotenv()

        set_key(env_file_path, "JIRA_API_KEY", auth.Jira_API_Key)
        set_key(env_file_path, "JIRA_EMAIL", auth.JIRA_EMAIL)
        set_key(env_file_path, "JIRA_DOMAIN", auth.JIRA_DOMAIN)
        # ❗ IMPORTANT: You must add your ngrok URL to your .env file for this to work
        # Example: set_key(env_file_path, "NGROK_BASE_URL", "https://your-id.ngrok-free.app")
        print("✅ Credentials saved to .env file.")

        print("🔄 Starting issue processing...")
        # This function will now use the credentials just saved to the .env file
        output =await process_all_issues() 
        print(f"✅ Issue processing complete. Output: {output}")

        print("🎣 Creating webhook...")
        create_jira_webhook()

    except Exception as e:
        print(f" An error occurred in the background task: {e}")


# --- API Endpoints ---

@app.post("/jira/connect")
async def connect_to_jira(auth: JiraAuthRequest, background_tasks: BackgroundTasks):
    """
    Receives and validates Jira credentials, then starts a background task
    to save config, fetch issues, and create a webhook.
    """
    if not await validate_jira_credentials(auth):
        raise HTTPException(
            status_code=401,
            detail="Invalid Jira credentials. Please check your API key, email, and domain.",
        )

    # Add the long-running job to the background
    background_tasks.add_task(background_sync_task, auth)

    # Return an immediate response to the user
    return {
        "message": "Jira credentials are valid. Starting background synchronization process."
    }


@app.post("/jira-webhook")
async def handle_jira_webhook(request: Request):
    """
    This is the single endpoint that receives all real-time updates from the Jira webhook.
    It now lives in the same application as the connect endpoint.
    """
    try:
        data = await request.json()
    except json.JSONDecodeError:
        print(" Failed to parse incoming JSON.")
        return {"status": "error", "reason": "Invalid JSON payload."}

    print("\n Webhook Triggered! Full Payload:")
    print(json.dumps(data, indent=2))
    
    # Your logic for handling the webhook data goes here
    # For now, we're just printing it.

    return {"status": "received"}


# --- Main Entry Point to Run the Server ---

if __name__ == "__main__":
    print("Starting FastAPI server...")
    # Make sure your .env file is loaded at startup if needed elsewhere
    load_dotenv()
    uvicorn.run(app, host="0.0.0.0", port=8000)