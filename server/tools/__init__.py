"""MCP Tools for Databricks operations."""

from .compute import load_compute_tools
from .core import load_core_tools
from .dashboards import load_dashboard_tools
from .data_management import load_data_tools
from .governance import load_governance_tools
from .jobs_pipelines import load_job_tools
from .mlflow import load_mlflow_tools
from .security import load_security_tools
from .sql_operations import load_sql_tools
from .unity_catalog import load_uc_tools
from .workspace import load_workspace_tools


def load_tools(mcp_server):
  """Register all MCP tools with the server.

  Args:
      mcp_server: The FastMCP server instance to register tools with
  """
  # Load tools from each module
  load_core_tools(mcp_server)
  load_sql_tools(mcp_server)
  load_uc_tools(mcp_server)
  load_data_tools(mcp_server)
  load_job_tools(mcp_server)
  load_dashboard_tools(mcp_server)
  load_compute_tools(mcp_server)
  load_security_tools(mcp_server)
  load_workspace_tools(mcp_server)
  load_mlflow_tools(mcp_server)
  load_governance_tools(mcp_server)
