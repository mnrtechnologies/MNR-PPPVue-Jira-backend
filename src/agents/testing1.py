from agents import Agent,Runner
from utils.prompt_loader import load_prompt
from pydantic import BaseModel
from typing import List,Dict
import asyncio
from openai import OpenAI
client=OpenAI()
from src.ingestion.fetch_issues import process_all_issues
class Output(BaseModel):
    issue_key:str
    ai_delay_prediction_score:float
    ai_delay_label:str
    ai_summary:str
    ai_priority_score:int


def agent_logic():    
     issues=process_all_issues()
     for issue in issues:
       resp = client.beta.chat.completions.parse(
       model="gpt-4o-2024-08-06",
       messages=[
        {"role": "system", "content": },
        {"role": "user", "content": issue}
    ],
    response_format=Output
         

)



if __name__ == "__main__":
    asyncio.run(agent_logic())
