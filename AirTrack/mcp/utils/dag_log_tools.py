from .base import mcp, AIRFLOW_API, AUTH
import requests

@mcp.tool()
def get_dag_run_logs(dag_id: str, run_id: str) -> str:
    """Get logs for a specific DAG run"""
    url = f"{AIRFLOW_API}/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
    response = requests.get(url, auth=AUTH)

    if response.status_code != 200:
        raise Exception(f"Unable to fetch DAG run logs: {response.status_code}")

    task_instances = response.json().get("task_instances", [])
    logs = []
    
    for task in task_instances:
        task_id = task.get("task_id")
        log_url = f"{AIRFLOW_API}/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1"
        log_response = requests.get(log_url, auth=AUTH)
        
        if log_response.status_code == 200:
            logs.append(log_response.text)
    
    return "\n".join(logs) 