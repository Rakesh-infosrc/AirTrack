from .base import mcp, AIRFLOW_API, AUTH, fetch_dag_runs
import requests

@mcp.tool()
def get_total_dags() -> int:
    """Return the total number of DAGs available in Airflow"""
    url = f"{AIRFLOW_API}/dags"
    response = requests.get(url, auth=AUTH)

    if response.status_code != 200:
        raise Exception("Unable to fetch DAG list")

    dags = response.json().get("dags", [])
    return len(dags)

@mcp.tool()
def get_total_runs(dag_id: str) -> int:
    """Return the total number of DAG runs for a specific DAG ID"""
    runs = fetch_dag_runs(dag_id)
    return len(runs) 