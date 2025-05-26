# server.py
from utils.base import mcp
from utils.all_tools import airflow_toolbox  # Import the unified tool

if __name__ == "__main__":
    # Start the MCP server
    mcp.run()
