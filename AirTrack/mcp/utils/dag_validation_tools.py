from .base import mcp, AIRFLOW_API, AUTH
import requests
from typing import List

@mcp.tool()
def list_all_dag_ids() -> List[str]:
    """
    Return a list of all available DAG IDs in Airflow.

    This function fetches DAGs from the Airflow API and returns a list
    of individual DAG IDs. If a concatenated dag_id string is detected,
    it splits it heuristically.
    """
    url = f"{AIRFLOW_API}/dags"
    response = requests.get(url, auth=AUTH)

    if response.status_code != 200:
        raise Exception(f"Unable to fetch DAG list: {response.status_code}")

    dags = response.json().get("dags", [])
    dag_ids = [dag["dag_id"] for dag in dags if "dag_id" in dag]
    return dag_ids

@mcp.tool()
def is_valid_dag_id(dag_id: str) -> bool:
    """Check if the provided DAG ID exists in Airflow."""
    return dag_id in list_all_dag_ids()
