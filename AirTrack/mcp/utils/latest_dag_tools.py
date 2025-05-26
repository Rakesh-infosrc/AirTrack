from .base import mcp, fetch_dag_runs

@mcp.tool()
def get_latest_dag_id(dag_id: str) -> str:
    """Get the latest DAG run ID"""
    runs = fetch_dag_runs(dag_id)
    return runs[-1].run_id if runs else "No runs found"

@mcp.tool()
def get_latest_start_time(dag_id: str) -> str:
    """Get the latest DAG run start time"""
    runs = fetch_dag_runs(dag_id)
    return runs[-1].start_date if runs else "N/A"

@mcp.tool()
def get_latest_end_time(dag_id: str) -> str:
    """Get the latest DAG run end time"""
    runs = fetch_dag_runs(dag_id)
    return runs[-1].end_date if runs else "N/A"

@mcp.tool()
def get_latest_duration(dag_id: str) -> float:
    """Get the latest DAG run duration in seconds"""
    runs = fetch_dag_runs(dag_id)
    return runs[-1].duration if runs else 0.0

@mcp.tool()
def get_latest_status(dag_id: str) -> str:
    """Get the latest DAG run status"""
    runs = fetch_dag_runs(dag_id)
    return runs[-1].status if runs else "N/A" 