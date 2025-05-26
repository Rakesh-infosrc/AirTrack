from .base import mcp, fetch_dag_runs
from .dag_validation_tools import list_all_dag_ids
from .dag_metadata_tools import get_dag_metadata

@mcp.tool()
def get_dags_summary(dag_ids: str) -> str:
    """
    Accepts comma-separated DAG IDs.
    Returns a markdown-style table format summarizing each DAG's metadata and latest run.
    if have multi dag make comparation table 
    
    """ 
    ids = [d.strip() for d in dag_ids.split(",")]
    valid_dags = set(list_all_dag_ids())

    table = (
        "| DAG ID          | Description            | Schedule Interval   | Status   | Tags                 | Total Runs | Latest Run ID       | Last Status | Last Start Time        | Last End Time          | Duration (sec) |\n"
        "|-----------------|------------------------|---------------------|----------|----------------------|------------|---------------------|-------------|------------------------|------------------------|----------------|\n"
    )

    for dag_id in ids:
        if dag_id not in valid_dags:
            table += f"| {dag_id} (Invalid DAG ID) | -                      | -                   | -        | -                    | -          | -                   | -           | -                      | -                      | -              |\n"
            continue

        metadata = get_dag_metadata(dag_id)
        description = (metadata.description[:20] + '...') if metadata.description and len(metadata.description) > 23 else (metadata.description or "-")
        schedule = metadata.schedule_interval or "-"
        status = "Active" if not metadata.is_paused else "Paused"
        tags = ", ".join(metadata.tags) if metadata.tags else "-"
        
        runs = fetch_dag_runs(dag_id)
        total_runs = len(runs) if runs else 0

        if not runs:
            table += (
                f"| {dag_id:<15} | {description:<22} | {schedule:<19} | {status:<8} | {tags:<20} | {total_runs:<10} | -                   | -           | -                      | -                      | -              |\n"
            )
            continue

        latest = runs[-1]
        last_run_id = latest.run_id or "-"
        last_status = latest.status or "-"
        last_start = latest.start_date or "-"
        last_end = latest.end_date or "-"
        duration = f"{latest.duration:.2f}" if latest.duration else "-"

        table += (
            f"| {dag_id:<15} | {description:<22} | {schedule:<19} | {status:<8} | {tags:<20} | {total_runs:<10} | {last_run_id:<19} | {last_status:<11} | {last_start:<22} | {last_end:<22} | {duration:<14} |\n"
        )

    return table 