import json
from textwrap import dedent
from openai import OpenAI
from pydantic import BaseModel

# The Output Pydantic model remains the same
class Output(BaseModel):
    ai_priority_score: float
    ai_delay_label: str
    ai_delay_score: float
    ai_summary: str

# Initialize the client (ensure your OPENAI_API_KEY is set as an environment variable)
client = OpenAI()
MODEL = "gpt-4o" # Using a more recent model like gpt-4o is recommended

pm_ai_prompt = '''
You are an AI project management assistant specializing in delay risk analysis for Jira issues. 

### Tasks
1. **AI Delay Likelihood Prediction**: Classify as one of:  
   - `"On Track"` (0-30% delay risk)  
   - `"At Risk"` (31-70% delay risk)  
   - `"Delayed"` (71-100% delay risk)  

2. **AI Delay Prediction Score**: Numeric score (0-1) representing the probability of delay.

3. **AI summary**: 1-2 sentence executive summary highlighting critical risk factors.

### Analysis Guidelines
Consider these factors when evaluating:
- Due date proximity and time sensitivity
- Priority level and blocked status
- Assignee workload and activity patterns
- Comment sentiment and unresolved threads
- Historical team velocity data
- Dependency chain completeness
- Progress vs. timeline consistency

### Output Requirements
Strictly use the JSON structure defined in the tools.
'''

def get_issue_data(data: dict) -> Output:
    """
    Calls the OpenAI API to analyze the issue data and returns a parsed Pydantic object.
    """
    completion = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": dedent(pm_ai_prompt)},
            {"role": "user", "content": json.dumps(data)},
        ],
        # Request a JSON response from the model
        response_format={"type": "json_object"},
    )
    
    # Extract the JSON string from the response
    response_json = completion.choices[0].message.content
    
    # Parse the JSON string into your Pydantic model
    # The .model_validate_json() method is a robust way to do this
    output = Output.model_validate_json(response_json)
    
    return output

def lambda_handler(event, context):
    try:
        # Directly use the event dict in local test
        body = event  
        output = get_issue_data(body)
        
        # This will now work correctly because 'output' is a Pydantic object
        # .model_dump_json(indent=2) makes the output nicely formatted for printing
        print(output)

    except Exception as e:
        print(f"Error processing record: {e}")

# Test it locally
if __name__ == "__main__":
    test_event = {
        'key': 'PVMVP-2',
        'project_name': 'AI-Powered Portfolio Dashboard',
        'worklog_enterie': 0,
        'team': '360 DC',
        'summary': 'building a dashboard with dark theme',
        'assignee': 'Pratik Mehakare',
        'reporter': 'Tousif ahmed',
        'labels': ['delay'],
        'original_estimate': '1w 3d 4h', # 72 hours
        'remaining_estimate': '2h 55m',
        'time_logged': '2d 1h 5m', # 17.08 hours
        'status': 'In Progress',
        'due_date': '2025-07-12',
        'updated_str': '2025-07-02T22:17:09.407-0500',
        'update_inactivity_days': -1,
        'priority': 'Medium'
    }

    lambda_handler(test_event, None)