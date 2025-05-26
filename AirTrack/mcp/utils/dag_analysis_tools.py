from .base import mcp
from .dag_log_tools import get_dag_run_logs

@mcp.tool()
def analyze_dag_run_issues(dag_id: str, run_id: str) -> str:
    """
    Analyze logs of a specific DAG run, detect errors or issues,
    and summarize them in a readable format.
    """
    logs = get_dag_run_logs(dag_id, run_id)
    if not logs:
        return f"No logs found for DAG '{dag_id}' run '{run_id}'."

    error_lines = []
    capture_traceback = False
    traceback_lines = []

    error_keywords = ["ERROR", "Exception", "Traceback", "Fail", "failed", "CRITICAL"]

    for line in logs.splitlines():
        if "Traceback" in line:
            capture_traceback = True
            traceback_lines = [line]
            continue

        if capture_traceback:
            if line.strip() == "" or not line.startswith(" "):
                capture_traceback = False
                error_lines.extend(traceback_lines)
                traceback_lines = []
            else:
                traceback_lines.append(line)
            continue

        if any(keyword in line for keyword in error_keywords):
            error_lines.append(line)

    if not error_lines:
        return f"No obvious errors or issues detected in logs for DAG '{dag_id}' run '{run_id}'."

    error_summary = "\n".join(error_lines)
    return f"### Detected issues in DAG '{dag_id}' run '{run_id}':\n```\n{error_summary}\n```" 