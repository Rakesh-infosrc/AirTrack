from .dag_validation_tools import list_all_dag_ids, is_valid_dag_id
from .dag_log_tools import get_dag_run_logs
from .dag_metadata_tools import get_dag_metadata
from .dag_analysis_tools import analyze_dag_run_issues
from .dag_stats_tools import get_total_dags, get_total_runs
from .dag_summary_tools import get_dags_summary
from .latest_dag_tools import (
    get_latest_dag_id, get_latest_start_time, get_latest_end_time,
    get_latest_duration, get_latest_status
)
from .base import mcp

@mcp.tool()
def airflow_toolbox(command: str, **kwargs):
    """
    Unified Airflow MCP tool. Specify 'command' and required arguments in kwargs.
    Available commands:
      - list_all_dag_ids
      - is_valid_dag_id
      - get_dag_run_logs
      - get_dag_metadata
      - analyze_dag_run_issues
      - get_total_dags
      - get_total_runs
      - get_dags_summary
      - get_latest_dag_id
      - get_latest_start_time
      - get_latest_end_time
      - get_latest_duration
      - get_latest_status
    """
    if command == "list_all_dag_ids":
        return list_all_dag_ids()
    elif command == "is_valid_dag_id":
        return is_valid_dag_id(kwargs["dag_id"])
    elif command == "get_dag_run_logs":
        return get_dag_run_logs(kwargs["dag_id"], kwargs["run_id"])
    elif command == "get_dag_metadata":
        return get_dag_metadata(kwargs["dag_id"])
    elif command == "analyze_dag_run_issues":
        return analyze_dag_run_issues(kwargs["dag_id"], kwargs["run_id"])
    elif command == "get_total_dags":
        return get_total_dags()
    elif command == "get_total_runs":
        return get_total_runs(kwargs["dag_id"])
    elif command == "get_dags_summary":
        return get_dags_summary(kwargs["dag_ids"])
    elif command == "get_latest_dag_id":
        return get_latest_dag_id(kwargs["dag_id"])
    elif command == "get_latest_start_time":
        return get_latest_start_time(kwargs["dag_id"])
    elif command == "get_latest_end_time":
        return get_latest_end_time(kwargs["dag_id"])
    elif command == "get_latest_duration":
        return get_latest_duration(kwargs["dag_id"])
    elif command == "get_latest_status":
        return get_latest_status(kwargs["dag_id"])
    else:
        raise ValueError(f"Unknown command: {command}")

airflow_toolbox(command="list_all_dag_ids") 