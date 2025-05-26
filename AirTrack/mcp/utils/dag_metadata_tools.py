from .base import mcp, AIRFLOW_API, AUTH
import requests

@mcp.tool()
def get_dag_metadata(dag_id: str) -> dict:
    """Get metadata for a specific DAG"""
    url = f"{AIRFLOW_API}/dags/{dag_id}"
    response = requests.get(url, auth=AUTH)

    if response.status_code != 200:
        raise Exception(f"Unable to fetch DAG metadata: {response.status_code}")

    return response.json() 