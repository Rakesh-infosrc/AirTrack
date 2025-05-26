from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional
import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

AIRFLOW_API = os.getenv("AIRFLOW_API")
AUTH = (os.getenv("AIRFLOW_USERNAME"), os.getenv("AIRFLOW_PASSWORD"))

# Initialize MCP server
mcp = FastMCP("AirflowMCP")

# Pydantic model for DAG info
class DagInfo(BaseModel):
    dag_id: str
    run_id: str
    start_date: Optional[str]
    end_date: Optional[str]
    duration: float
    status: str

@mcp.tool()
def fetch_dag_runs(dag_id: str) -> List[DagInfo]:
    url = f"{AIRFLOW_API}/dags/{dag_id}/dagRuns"
    response = requests.get(url, auth=AUTH)

    if response.status_code != 200:
        raise Exception("Unable to fetch DAG info")

    dag_runs = response.json().get("dag_runs", [])
    result = []

    for run in dag_runs:
        start = run.get("start_date")
        end = run.get("end_date")
        duration = 0.0
        if start and end:
            start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
            duration = (end_dt - start_dt).total_seconds()

        result.append(DagInfo(
            dag_id=run.get("dag_id"),
            run_id=run.get("dag_run_id"),
            start_date=start,
            end_date=end,
            duration=duration,
            status=run.get("state"),
        ))

    return result 