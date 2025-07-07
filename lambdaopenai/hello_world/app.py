import json
from textwrap import dedent
import os
import logging
import time
import random
from typing import Dict, List, Any, Optional
from openai import OpenAI, APIError
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from pydantic import BaseModel, ValidationError

# --- Basic Configuration ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Pydantic Output Model ---
class AiPrediction(BaseModel):
    ai_delay_label: str
    ai_delay_score: float
    ai_summary: str
    ai_priority_score: float

# --- Environment Variable Loading ---
try:
    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
    MONGO_URI = os.environ["MONGO_URI"]
    DB_NAME = os.environ.get("DB_NAME",)
    COLLECTION_NAME = os.environ.get("COLLECTION_NAME_ISSUES")
    MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o")
    MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
    RETRY_BASE_DELAY = float(os.environ.get("RETRY_BASE_DELAY", "1.0"))
except KeyError as e:
    logger.fatal(f"FATAL: Missing essential environment variable: {e}")
    raise e

# --- Client Initialization ---
# Initialize OpenAI client outside handler (safe)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# MongoDB client initialization (without connection test)
mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]

# --- AI Prompt ---
pm_ai_prompt = """
### Role
You are an AI Project Risk Analyst specializing in Jira issue evaluation. Analyze the following issue data to predict delivery risks and business impact.

### Input Data Structure
{
  "key": "Issue identifier",
  "project_name": "Project name",
  "worklog_entries": "List of worklogs",
  "team": "Assigned team",
  "summary": "Issue description",
  "assignee": "Assignee name (null if unassigned)",
  "reporter": "Reporter name",
  "labels": ["Label1", "Label2"],
  "original_estimate": "Original time estimate (seconds)",
  "remaining_estimate": "Remaining time estimate (seconds)",
  "time_logged": "Total time spent (seconds)",
  "status": "Current status",
  "due_date": "Due date (YYYY-MM-DD or null)",
  "update_inactivity_days": "Days since last update",
  "priority": "Priority level"
}

### Output Requirements
Return a valid JSON object with these exact keys:
{
  "ai_delay_label": "<On Track|At Risk|Delayed>",
  "ai_delay_score": <float between 0.00 and 1.00>,
  "ai_summary": "<A 1-2 sentence risk analysis summary>",
  "ai_priority_score": <float between 0.00 and 1.00>
}

### Prediction Framework
1. **Delay Assessment** (Combine these factors):
   - `Time Pressure` = Due date proximity (escalate if <3 days)
   - `Progress Health` = (Time_logged / (Time_logged + Remaining_estimate))
   - `Activity Risk` = Update_inactivity_days > 7 → High risk
   - `Resource Risk` = Unassigned OR (High worklog entries + Low progress)
   - `Blocked Status` = "Blocked" in status OR "blocked" in labels

2. **Priority Assessment** (Business Impact):
   - Base: Map priority to 0.0-1.0 scale (Critical=0.9, High=0.7, Medium=0.5, Low=0.3)
   - Boosters:
     +0.2 if due in <3 days
     +0.3 if blocks other issues (check labels for "blocker")
     +0.1 per critical label ("security", "compliance", "legal")

### Edge Case Handling Rules
| Scenario | Action |
|----------|--------|
| **Unassigned issue** | Automatic "Delayed" (score=0.95) |
| **No due date** | Use 2x average team cycle time as proxy (default 14 days) |
| **Zero estimates** | Calculate progress ratio using status transitions |
| **Closed issues** | "On Track" (0.00) with priority_score=0.0 |
| **Negative remaining**| Treat as "Delayed" (score=1.0) |
| **High inactivity** | >14 days inactivity → "Delayed" regardless of status |

### Calculation Guidelines
```mermaid
graph LR
    A[Start] --> B{Unassigned?}
    B -->|Yes| C["Delayed (0.95)"]
    B -->|No| D{Closed?}
    D -->|Yes| E["On Track (0.00)"]
    D -->|No| F[Calculate Score]
    F --> G["Time Pressure (30%)"]
    F --> H["Progress Health (25%)"]
    F --> I["Activity Risk (20%)"]
    F --> J["Blocked Status (15%)"]
    F --> K["Priority Level (10%)"]
    G --> L[Combine Scores]
    H --> L
    I --> L
    J --> L
    K --> L
    L --> M{Apply Overrides?}
    M -->|Past due| N["Delayed (1.0)"]
    M -->|Inactive>14d| N
    M -->|None| O[Final Label]
```
"""

# --- Utility Functions ---
def exponential_backoff_sleep(attempt: int, base_delay: float = RETRY_BASE_DELAY) -> None:
    """
    Sleep with exponential backoff and jitter.
    
    Args:
        attempt: Current attempt number (0-based)
        base_delay: Base delay in seconds
    """
    delay = base_delay * (2 ** attempt)
    # Add jitter to prevent thundering herd
    jitter = random.uniform(0.1, 0.5)
    total_delay = delay + jitter
    logger.info(f"Retrying in {total_delay:.2f} seconds...")
    time.sleep(total_delay)

def test_mongodb_connection() -> bool:
    """
    Test MongoDB connection with retries.
    
    Returns:
        True if connection successful, False otherwise
    """
    for attempt in range(MAX_RETRIES):
        try:
            # Test connection with a simple command
            mongo_client.admin.command('ping')
            logger.info("MongoDB connection test successful")
            return True
        except ConnectionFailure as e:
            logger.warning(f"MongoDB connection attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                exponential_backoff_sleep(attempt)
            else:
                logger.error("All MongoDB connection attempts failed")
                return False
    return False

def get_ai_predictions_with_retry(issue_data: dict) -> Optional[dict]:
    """
    Calls the OpenAI API to get risk predictions with retry logic.
    
    Args:
        issue_data: A dictionary containing the Jira issue details.

    Returns:
        A dictionary with the AI's predictions, or None if all retries failed.
    """
    for attempt in range(MAX_RETRIES):
        try:
            completion = openai_client.chat.completions.create(
                model=MODEL,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": dedent(pm_ai_prompt)},
                    {"role": "user", "content": json.dumps(issue_data)},
                ],
            )
            
            response_content = completion.choices[0].message.content
            parsed_output = AiPrediction.model_validate_json(response_content)
            
            return parsed_output.model_dump()
            
        except APIError as e:
            logger.warning(f"OpenAI API attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                exponential_backoff_sleep(attempt)
            else:
                logger.error("All OpenAI API attempts failed")
                return None
        except ValidationError as e:
            logger.error(f"Pydantic validation error (not retryable): {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in AI prediction (not retryable): {e}")
            return None
    
    return None

def store_document_with_retry(document: dict, issue_key: str) -> bool:
    """
    Store document in MongoDB with retry logic.
    
    Args:
        document: Document to store
        issue_key: Issue key for logging
        
    Returns:
        True if successful, False otherwise
    """
    for attempt in range(MAX_RETRIES):
        try:
            update_result = collection.update_one(
                {'key': issue_key},
                {'$set': document},
                upsert=True
            )
            
            if update_result.upserted_id:
                logger.info(f"Successfully INSERTED document for issue {issue_key} with new ID: {update_result.upserted_id}")
            else:
                logger.info(f"Successfully UPDATED document for issue {issue_key}. Matched: {update_result.matched_count}, Modified: {update_result.modified_count}")
            
            return True
            
        except OperationFailure as e:
            logger.warning(f"MongoDB operation attempt {attempt + 1} failed for {issue_key}: {e}")
            if attempt < MAX_RETRIES - 1:
                exponential_backoff_sleep(attempt)
            else:
                logger.error(f"All MongoDB operation attempts failed for {issue_key}")
                return False
        except Exception as e:
            logger.error(f"Unexpected MongoDB error for {issue_key}: {e}")
            return False
    
    return False

def process_single_record(record: dict) -> dict:
    """
    Process a single SQS record and return the result.
    
    Args:
        record: SQS record to process
        
    Returns:
        Dictionary with processing result
    """
    result = {
        'success': False,
        'error': None,
        'issue_key': 'Unknown',
        'record_id': record.get('messageId', 'Unknown')
    }
    
    try:
        # 1. Parse SQS message
        body_str = record.get('body')
        if not body_str:
            result['error'] = "Empty message body"
            return result
            
        issue_data = json.loads(body_str)
        issue_key = issue_data.get("key", "Unknown")
        result['issue_key'] = issue_key
        
        logger.info(f"Processing issue: {issue_key}")

        # 2. Get AI predictions with retry
        logger.info(f"Requesting AI analysis for {issue_key}...")
        ai_predictions = get_ai_predictions_with_retry(issue_data)
        
        if ai_predictions is None:
            result['error'] = "Failed to get AI predictions after retries"
            return result

        logger.info(f"Successfully received AI predictions for {issue_key}")

        # 3. Combine data
        document_to_store = {
            **issue_data,
            **ai_predictions
        }

        # 4. Store in MongoDB with retry
        logger.info(f"Saving document for {issue_key} to MongoDB...")
        if store_document_with_retry(document_to_store, issue_key):
            result['success'] = True
            logger.info(f"Successfully processed issue {issue_key}")
        else:
            result['error'] = "Failed to store document after retries"

    except json.JSONDecodeError as e:
        result['error'] = f"JSON decode error: {str(e)}"
        logger.error(f"JSONDecodeError: Failed to parse SQS message body. Body: '{body_str}'. Error: {e}")
    
    except Exception as e:
        result['error'] = f"Unexpected error: {str(e)}"
        logger.error(f"Unexpected error processing record: {e}", exc_info=True)
    
    return result

def lambda_handler(event, context):
    """
    Main Lambda handler with partial batch failure support.
    """
    # Test MongoDB connection at handler start
    if not test_mongodb_connection():
        logger.error("MongoDB connection failed - cannot process records")
        # Return all records as failed
        failed_records = [
            {'itemIdentifier': record.get('messageId', 'Unknown')} 
            for record in event.get('Records', [])
        ]
        return {
            'statusCode': 500,
            'batchItemFailures': failed_records,
            'body': json.dumps('MongoDB connection failed')
        }
    
    records = event.get('Records', [])
    if not records:
        logger.warning("No records to process")
        return {
            'statusCode': 200,
            'body': json.dumps('No records to process')
        }
    
    logger.info(f"Processing {len(records)} records")
    
    # Process each record and track results
    batch_item_failures = []
    successful_count = 0
    
    for record in records:
        result = process_single_record(record)
        
        if result['success']:
            successful_count += 1
        else:
            # Add to batch failures for SQS partial batch failure handling
            batch_item_failures.append({
                'itemIdentifier': result['record_id']
            })
            logger.error(f"Failed to process record {result['record_id']} (issue: {result['issue_key']}): {result['error']}")
    
    # Log summary
    total_records = len(records)
    failed_count = len(batch_item_failures)
    logger.info(f"Processing complete. Success: {successful_count}, Failed: {failed_count}, Total: {total_records}")
    
    # Return response with partial batch failure info
    response = {
        'statusCode': 200 if successful_count > 0 else 500,
        'body': json.dumps({
            'message': 'Processing complete',
            'successful': successful_count,
            'failed': failed_count,
            'total': total_records
        })
    }
    
    # Include batch failures for SQS to retry only failed messages
    if batch_item_failures:
        response['batchItemFailures'] = batch_item_failures
    
    return response