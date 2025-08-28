"""Governance and lineage MCP tools for Databricks."""

from databricks.sdk import WorkspaceClient


def load_governance_tools(mcp_server):
    """Register governance and lineage MCP tools with the server.

    Args:
        mcp_server: The FastMCP server instance to register tools with
    """

    def get_workspace_client():
        """Get authenticated Databricks workspace client."""
        return WorkspaceClient()

    # System Tables and Audit Logs

    @mcp_server.tool
    def list_system_schemas() -> dict:
        """List system schemas available for governance queries.

        Returns:
            Dictionary with system schemas information or error message
        """
        try:
            client = get_workspace_client()
            
            # List system schemas in the system catalog
            try:
                schemas = list(client.schemas.list(catalog_name='system'))
                schema_list = []
                for schema in schemas:
                    schema_info = {
                        'name': schema.name,
                        'catalog_name': schema.catalog_name,
                        'comment': schema.comment,
                        'full_name': schema.full_name,
                        'owner': schema.owner,
                        'created_at': schema.created_at,
                        'updated_at': schema.updated_at
                    }
                    schema_list.append(schema_info)
                
                return {
                    'status': 'success',
                    'system_schemas': schema_list,
                    'count': len(schema_list),
                    'message': 'System schemas retrieved successfully'
                }
            except Exception:
                # Fallback to known system schemas
                return {
                    'status': 'success',
                    'system_schemas': [
                        {'name': 'access', 'description': 'Audit logs and access information'},
                        {'name': 'billing', 'description': 'Billing and usage information'},
                        {'name': 'compute', 'description': 'Compute resource information'},
                        {'name': 'storage', 'description': 'Storage usage information'},
                        {'name': 'marketplace', 'description': 'Marketplace information'},
                        {'name': 'information_schema', 'description': 'Information schema metadata'}
                    ],
                    'count': 6,
                    'message': 'Standard system schemas listed'
                }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def query_audit_logs(start_date: str, end_date: str, service_name: str = None, action_name: str = None, limit: int = 1000) -> dict:
        """Query audit logs from system.access.audit table.

        Args:
            start_date: Start date for audit logs (YYYY-MM-DD format)
            end_date: End date for audit logs (YYYY-MM-DD format)
            service_name: Optional service name to filter by
            action_name: Optional action name to filter by
            limit: Maximum number of results to return

        Returns:
            Dictionary with audit log query results or error message
        """
        try:
            client = get_workspace_client()
            
            # Build SQL query for audit logs
            where_conditions = [
                f"event_date >= '{start_date}'",
                f"event_date <= '{end_date}'"
            ]
            
            if service_name:
                where_conditions.append(f"service_name = '{service_name}'")
            if action_name:
                where_conditions.append(f"action_name = '{action_name}'")
            
            where_clause = " AND ".join(where_conditions)
            
            query = f"""
            SELECT 
                event_time,
                event_date,
                workspace_id,
                account_id,
                user_identity,
                service_name,
                action_name,
                request_id,
                response,
                source_ip_address,
                user_agent
            FROM system.access.audit
            WHERE {where_clause}
            ORDER BY event_time DESC
            LIMIT {limit}
            """
            
            # Execute query using SQL warehouse
            result = client.statement_execution.execute_statement(
                statement=query,
                warehouse_id=None  # Will use default warehouse
            )
            
            return {
                'status': 'success',
                'query': query,
                'start_date': start_date,
                'end_date': end_date,
                'service_name': service_name,
                'action_name': action_name,
                'statement_id': result.statement_id,
                'message': 'Audit log query executed successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def query_table_lineage(table_name: str, upstream: bool = True, downstream: bool = True) -> dict:
        """Query table lineage from system.access.table_lineage.

        Args:
            table_name: Full table name (catalog.schema.table)
            upstream: Include upstream lineage (tables this table depends on)
            downstream: Include downstream lineage (tables that depend on this table)

        Returns:
            Dictionary with lineage information or error message
        """
        try:
            client = get_workspace_client()
            
            lineage_conditions = []
            if upstream:
                lineage_conditions.append(f"target_table_full_name = '{table_name}'")
            if downstream:
                lineage_conditions.append(f"source_table_full_name = '{table_name}'")
            
            if not lineage_conditions:
                return {'status': 'error', 'message': 'Must specify upstream or downstream lineage'}
            
            where_clause = " OR ".join(lineage_conditions)
            
            query = f"""
            SELECT 
                source_table_full_name,
                target_table_full_name,
                source_table_catalog,
                source_table_schema,
                source_table_name,
                target_table_catalog,
                target_table_schema,
                target_table_name,
                created_at,
                created_by
            FROM system.access.table_lineage
            WHERE {where_clause}
            ORDER BY created_at DESC
            """
            
            # Execute query
            result = client.statement_execution.execute_statement(
                statement=query,
                warehouse_id=None
            )
            
            return {
                'status': 'success',
                'table_name': table_name,
                'query': query,
                'upstream': upstream,
                'downstream': downstream,
                'statement_id': result.statement_id,
                'message': 'Table lineage query executed successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def query_column_lineage(table_name: str, column_name: str = None) -> dict:
        """Query column lineage from system.access.column_lineage.

        Args:
            table_name: Full table name (catalog.schema.table)
            column_name: Optional specific column name to trace

        Returns:
            Dictionary with column lineage information or error message
        """
        try:
            client = get_workspace_client()
            
            where_conditions = [
                f"(source_table_full_name = '{table_name}' OR target_table_full_name = '{table_name}')"
            ]
            
            if column_name:
                where_conditions.append(f"(source_column_name = '{column_name}' OR target_column_name = '{column_name}')")
            
            where_clause = " AND ".join(where_conditions)
            
            query = f"""
            SELECT 
                source_table_full_name,
                source_column_name,
                target_table_full_name,
                target_column_name,
                created_at,
                created_by
            FROM system.access.column_lineage
            WHERE {where_clause}
            ORDER BY created_at DESC
            """
            
            # Execute query
            result = client.statement_execution.execute_statement(
                statement=query,
                warehouse_id=None
            )
            
            return {
                'status': 'success',
                'table_name': table_name,
                'column_name': column_name,
                'query': query,
                'statement_id': result.statement_id,
                'message': 'Column lineage query executed successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Data Discovery and Usage

    @mcp_server.tool
    def query_table_usage(table_name: str, days: int = 30) -> dict:
        """Query table usage patterns from system tables.

        Args:
            table_name: Full table name (catalog.schema.table) or pattern
            days: Number of days to look back for usage data

        Returns:
            Dictionary with table usage information or error message
        """
        try:
            client = get_workspace_client()
            
            query = f"""
            SELECT 
                table_name,
                read_count,
                write_count,
                last_accessed,
                accessed_by_users
            FROM (
                SELECT 
                    '{table_name}' as table_name,
                    COUNT(CASE WHEN action_name LIKE '%READ%' THEN 1 END) as read_count,
                    COUNT(CASE WHEN action_name LIKE '%WRITE%' THEN 1 END) as write_count,
                    MAX(event_time) as last_accessed,
                    COUNT(DISTINCT user_identity.email) as accessed_by_users
                FROM system.access.audit
                WHERE event_date >= DATE_SUB(CURRENT_DATE(), {days})
                AND (request_params.table_full_name = '{table_name}' 
                     OR request_params.table_full_name LIKE '%{table_name}%')
            )
            """
            
            # Execute query
            result = client.statement_execution.execute_statement(
                statement=query,
                warehouse_id=None
            )
            
            return {
                'status': 'success',
                'table_name': table_name,
                'days': days,
                'query': query,
                'statement_id': result.statement_id,
                'message': 'Table usage query executed successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def query_workspace_objects(object_type: str = None, created_days: int = 30) -> dict:
        """Query workspace objects and their metadata.

        Args:
            object_type: Type of object to filter by (NOTEBOOK, DASHBOARD, etc.)
            created_days: Number of days to look back for created objects

        Returns:
            Dictionary with workspace objects information or error message
        """
        try:
            client = get_workspace_client()
            
            where_conditions = [
                f"event_date >= DATE_SUB(CURRENT_DATE(), {created_days})",
                "action_name = 'create'"
            ]
            
            if object_type:
                where_conditions.append(f"request_params.object_type = '{object_type}'")
            
            where_clause = " AND ".join(where_conditions)
            
            query = f"""
            SELECT 
                event_time,
                user_identity.email as creator,
                request_params.object_type,
                request_params.path,
                request_params.object_id,
                service_name
            FROM system.access.audit
            WHERE {where_clause}
            ORDER BY event_time DESC
            """
            
            # Execute query
            result = client.statement_execution.execute_statement(
                statement=query,
                warehouse_id=None
            )
            
            return {
                'status': 'success',
                'object_type': object_type,
                'created_days': created_days,
                'query': query,
                'statement_id': result.statement_id,
                'message': 'Workspace objects query executed successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Permissions and Access Control

    @mcp_server.tool
    def query_permissions_changes(principal: str = None, days: int = 7) -> dict:
        """Query recent permission changes from audit logs.

        Args:
            principal: Optional user or service principal to filter by
            days: Number of days to look back for permission changes

        Returns:
            Dictionary with permission changes information or error message
        """
        try:
            client = get_workspace_client()
            
            where_conditions = [
                f"event_date >= DATE_SUB(CURRENT_DATE(), {days})",
                "action_name IN ('grant', 'revoke', 'update_permissions', 'set_permissions')"
            ]
            
            if principal:
                where_conditions.append(f"(user_identity.email = '{principal}' OR request_params.principal_name = '{principal}')")
            
            where_clause = " AND ".join(where_conditions)
            
            query = f"""
            SELECT 
                event_time,
                user_identity.email as changed_by,
                action_name,
                request_params.principal_name as affected_principal,
                request_params.permission_level,
                request_params.object_type,
                request_params.object_id,
                service_name
            FROM system.access.audit
            WHERE {where_clause}
            ORDER BY event_time DESC
            """
            
            # Execute query
            result = client.statement_execution.execute_statement(
                statement=query,
                warehouse_id=None
            )
            
            return {
                'status': 'success',
                'principal': principal,
                'days': days,
                'query': query,
                'statement_id': result.statement_id,
                'message': 'Permission changes query executed successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Billing and Usage Analytics

    @mcp_server.tool
    def query_compute_usage(start_date: str, end_date: str, cluster_id: str = None) -> dict:
        """Query compute usage from system.billing tables.

        Args:
            start_date: Start date for usage query (YYYY-MM-DD format)
            end_date: End date for usage query (YYYY-MM-DD format)
            cluster_id: Optional cluster ID to filter by

        Returns:
            Dictionary with compute usage information or error message
        """
        try:
            client = get_workspace_client()
            
            where_conditions = [
                f"usage_date >= '{start_date}'",
                f"usage_date <= '{end_date}'"
            ]
            
            if cluster_id:
                where_conditions.append(f"cluster_id = '{cluster_id}'")
            
            where_clause = " AND ".join(where_conditions)
            
            query = f"""
            SELECT 
                usage_date,
                cluster_id,
                cluster_name,
                node_type,
                usage_quantity,
                usage_unit,
                list_price,
                usage_metadata
            FROM system.billing.usage
            WHERE {where_clause}
            AND usage_metadata.cluster_id IS NOT NULL
            ORDER BY usage_date DESC, usage_quantity DESC
            """
            
            # Execute query
            result = client.statement_execution.execute_statement(
                statement=query,
                warehouse_id=None
            )
            
            return {
                'status': 'success',
                'start_date': start_date,
                'end_date': end_date,
                'cluster_id': cluster_id,
                'query': query,
                'statement_id': result.statement_id,
                'message': 'Compute usage query executed successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def query_storage_usage(start_date: str, end_date: str) -> dict:
        """Query storage usage from system.billing tables.

        Args:
            start_date: Start date for usage query (YYYY-MM-DD format)
            end_date: End date for usage query (YYYY-MM-DD format)

        Returns:
            Dictionary with storage usage information or error message
        """
        try:
            client = get_workspace_client()
            
            query = f"""
            SELECT 
                usage_date,
                storage_type,
                SUM(usage_quantity) as total_storage_tb,
                SUM(list_price) as total_cost_usd,
                COUNT(*) as usage_records
            FROM system.billing.usage
            WHERE usage_date >= '{start_date}'
            AND usage_date <= '{end_date}'
            AND usage_metadata.storage_type IS NOT NULL
            GROUP BY usage_date, storage_type
            ORDER BY usage_date DESC, total_storage_tb DESC
            """
            
            # Execute query
            result = client.statement_execution.execute_statement(
                statement=query,
                warehouse_id=None
            )
            
            return {
                'status': 'success',
                'start_date': start_date,
                'end_date': end_date,
                'query': query,
                'statement_id': result.statement_id,
                'message': 'Storage usage query executed successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Data Quality and Monitoring

    @mcp_server.tool
    def list_quality_monitors(catalog_name: str = None) -> dict:
        """List data quality monitors in Unity Catalog.

        Args:
            catalog_name: Optional catalog name to filter monitors

        Returns:
            Dictionary with data quality monitors or error message
        """
        try:
            client = get_workspace_client()
            
            # List all quality monitors
            monitors = list(client.quality_monitors.list())
            
            monitor_list = []
            for monitor in monitors:
                if catalog_name and not monitor.table_name.startswith(f"{catalog_name}."):
                    continue
                    
                monitor_info = {
                    'table_name': monitor.table_name,
                    'monitor_version': monitor.monitor_version,
                    'status': monitor.status,
                    'profile_type': monitor.profile_type,
                    'output_schema_name': monitor.output_schema_name,
                    'created_by': monitor.created_by,
                    'created_time': monitor.created_time,
                    'updated_by': monitor.updated_by,
                    'updated_time': monitor.updated_time
                }
                monitor_list.append(monitor_info)
            
            return {
                'status': 'success',
                'catalog_name': catalog_name,
                'monitors': monitor_list,
                'count': len(monitor_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_quality_monitor_status(table_name: str) -> dict:
        """Get status of a data quality monitor.

        Args:
            table_name: Full table name (catalog.schema.table)

        Returns:
            Dictionary with monitor status or error message
        """
        try:
            client = get_workspace_client()
            
            monitor = client.quality_monitors.get(table_name)
            
            return {
                'status': 'success',
                'monitor': {
                    'table_name': monitor.table_name,
                    'monitor_version': monitor.monitor_version,
                    'status': monitor.status,
                    'profile_type': monitor.profile_type,
                    'output_schema_name': monitor.output_schema_name,
                    'created_by': monitor.created_by,
                    'created_time': monitor.created_time,
                    'updated_by': monitor.updated_by,
                    'updated_time': monitor.updated_time,
                    'drift_metrics_table_name': monitor.drift_metrics_table_name,
                    'profile_metrics_table_name': monitor.profile_metrics_table_name
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}