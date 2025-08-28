# Databricks MCP Tools - Comprehensive REST API Coverage

This directory contains a comprehensive set of MCP tools covering the entire Databricks REST API surface. The tools are organized into logical, manageable modules for maintainability and ease of use.

## Structure

```
server/tools/
├── __init__.py              # Main entry point that imports and registers all tools
├── core.py                  # Core health and basic operations (1 tool)
├── sql_operations.py        # SQL warehouse and query management (15 tools)
├── unity_catalog.py         # Unity Catalog operations (20 tools)
├── data_management.py       # DBFS, volumes, and storage operations (16 tools)
├── jobs_pipelines.py        # Job and DLT pipeline management (20 tools)
├── dashboards.py            # Dashboard and monitoring tools (8 tools)
├── compute.py               # Compute clusters and instance pools (25 tools)
├── security.py              # Security, users, groups, and tokens (32 tools)
├── workspace.py             # Workspace files, repos, and permissions (21 tools)
├── mlflow.py                # MLflow model registry and experiments (20 tools)
├── governance.py            # System tables, audit logs, and lineage (12 tools)
└── utils.py                 # Utility functions and helpers
```

## Tool Distribution

| Module | Tools | Description |
|--------|-------|-------------|
| **core.py** | 1 | Health checks and basic workspace connectivity |
| **compute.py** | 25 | Cluster management, instance pools, libraries, policies, node types |
| **security.py** | 32 | Secrets, users, groups, service principals, tokens, IP access lists |
| **workspace.py** | 21 | Workspace objects, Git repos, permissions, settings |
| **mlflow.py** | 20 | Model registry, experiments, runs, model versions |
| **unity_catalog.py** | 20 | Catalogs, schemas, tables, volumes, functions, models |
| **jobs_pipelines.py** | 20 | Job management, DLT pipelines, runs, schedules |
| **data_management.py** | 16 | DBFS operations, external locations, storage credentials |
| **governance.py** | 12 | System tables, audit logs, lineage, data quality |
| **dashboards.py** | 8 | Lakeview and legacy dashboard management |
| **sql_operations.py** | 15 | SQL warehouse management, query execution |

**Total: 190 tools** covering the complete Databricks REST API

## Complete Databricks API Coverage

This comprehensive tool set provides access to all major Databricks REST API endpoints:

### Compute & Infrastructure
- **Clusters**: Create, start, stop, restart, delete, configure, monitor
- **Instance Pools**: Manage instance pools for cost optimization
- **Libraries**: Install/uninstall packages on clusters
- **Cluster Policies**: Define and manage cluster governance policies
- **Node Types & Spark Versions**: Query available infrastructure options

### Security & Identity Management
- **Secrets**: Complete secret scope and secret management
- **Users & Groups**: Full SCIM user/group lifecycle management
- **Service Principals**: Application identity management
- **Personal Access Tokens**: Token lifecycle management
- **IP Access Lists**: Network security controls
- **Workspace Configuration**: Security settings management

### Data & Storage
- **DBFS**: File system operations (upload, download, list, delete)
- **Unity Catalog**: Full catalog, schema, table, volume management
- **External Locations**: Cloud storage integration
- **Storage Credentials**: Cloud credential management
- **Data Lineage**: Track data dependencies and relationships

### Workspace & Development
- **Workspace Objects**: Notebooks, files, and directory management
- **Git Repositories**: Clone, pull, branch management
- **Permissions**: Fine-grained access control across all objects
- **Current User**: User profile and settings

### ML & Data Science
- **MLflow Model Registry**: Model lifecycle management
- **MLflow Experiments**: Experiment tracking and management
- **MLflow Runs**: Run lifecycle and artifact management
- **Model Versioning**: Model staging and deployment workflows

### Jobs & Orchestration
- **Jobs**: Complete job lifecycle management
- **DLT Pipelines**: Delta Live Tables pipeline operations
- **Job Runs**: Execution monitoring and management
- **Schedules**: Job scheduling and triggers

### Analytics & BI
- **SQL Warehouses**: Serverless compute management
- **Query Execution**: SQL query operations and monitoring
- **Dashboards**: Lakeview and legacy dashboard management
- **Query History**: Query performance and usage analytics

### Governance & Monitoring
- **System Tables**: Query audit logs, usage, billing data
- **Data Quality Monitors**: Data quality tracking and alerts
- **Usage Analytics**: Compute and storage usage patterns
- **Permission Auditing**: Track permission changes and access

## Benefits of This Implementation

1. **Complete Coverage**: Access to 190+ tools covering all Databricks APIs
2. **Modular Design**: Organized into logical domains for easy navigation
3. **Consistent Patterns**: All tools follow the same simple, direct patterns
4. **Error Handling**: Comprehensive error handling with clear messages
5. **Type Safety**: Full type annotations for better IDE support
6. **Documentation**: Detailed docstrings for every tool
7. **Maintainability**: Simple, readable code following project guidelines

## Usage

The main `load_tools()` function in `__init__.py` automatically registers all 190+ tools with your MCP server. All tools are available immediately with no additional configuration required.

```python
from server.tools import load_tools
load_tools(mcp_server)
```

## Tool Patterns

All tools follow consistent patterns:
- Simple function signatures with clear parameters
- Direct Databricks SDK calls (no wrappers or abstractions)
- Consistent return format: `{'status': 'success'/'error', 'message': '...', ...}`
- Proper error handling with meaningful error messages
- Full type annotations for better development experience
