import json
from textwrap import dedent
import os
from openai import OpenAI
from src.logger import get_logger
logger=get_logger(__name__)
from pydantic import BaseModel
class Output(BaseModel):
        ai_delay_label: str
        ai_delay_score:float
        ai_summary: str
        ai_priority_score:float
client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
MODEL="o1"

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
Return JSON with these keys:
{
  "delay_likelihood_label": "<On Track|At Risk|Delayed>",
  "delay_prediction_score": <float 0.00-1.00>,
  "summary": "<1-2 sentence risk analysis>"
  "priority_score": <float 0.00-1.00>,
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
| **Negative remaining** | Treat as "Delayed" (score=1.0) |
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


"""
# Set your OpenAI API Key from environment variable
def get_issue_data(data:dict):
    completion = client.beta.chat.completions.parse(
        model=MODEL,
        messages=[
            {"role": "system", "content": dedent(pm_ai_prompt)},
            {"role": "user", "content": json.dumps(data)},
        ],
        response_format=Output,
    )

    return completion.choices[0].message

def lambda_handler(event, context):

    for record in event['Records']:
        try:
            body_str = record['body']  # SQS sends body as string
            body = json.loads(body_str)   # Get SQS body

            output=get_issue_data(body)
            parsed_data=output.parsed
            data={
            "ai_delay_prediction_score":parsed_data.ai_delay_score, 
            "ai_delay_label":parsed_data.ai_delay_label,     
            "ai_priority_score":parsed_data.ai_priority_score,
            "ai_summary":parsed_data.ai_summary
             }
            print(data)
            print(event)
            logger.info(data)

        except Exception as e:
            print("Error processing record:", e)





