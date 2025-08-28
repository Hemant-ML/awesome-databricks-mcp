"""Compute management MCP tools for Databricks."""

import os

from databricks.sdk import WorkspaceClient


def load_compute_tools(mcp_server):
    """Register compute management MCP tools with the server.

    Args:
        mcp_server: The FastMCP server instance to register tools with
    """

    def get_workspace_client():
        """Get authenticated Databricks workspace client."""
        return WorkspaceClient()

    # Clusters API Tools

    @mcp_server.tool
    def list_clusters() -> dict:
        """List all clusters in the Databricks workspace.

        Returns:
            Dictionary containing list of clusters with their details
        """
        try:
            client = get_workspace_client()
            clusters = list(client.clusters.list())
            
            cluster_list = []
            for cluster in clusters:
                cluster_info = {
                    'cluster_id': cluster.cluster_id,
                    'cluster_name': cluster.cluster_name,
                    'state': cluster.state.name if cluster.state else None,
                    'node_type_id': cluster.node_type_id,
                    'driver_node_type_id': cluster.driver_node_type_id,
                    'num_workers': cluster.num_workers,
                    'autoscale': cluster.autoscale.dict() if cluster.autoscale else None,
                    'spark_version': cluster.spark_version,
                    'creator_user_name': cluster.creator_user_name,
                    'start_time': cluster.start_time,
                    'terminated_time': cluster.terminated_time,
                    'last_state_loss_time': cluster.last_state_loss_time,
                    'last_activity_time': cluster.last_activity_time
                }
                cluster_list.append(cluster_info)

            return {
                'status': 'success',
                'clusters': cluster_list,
                'count': len(cluster_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_cluster(cluster_id: str) -> dict:
        """Get detailed information about a specific cluster.

        Args:
            cluster_id: The ID of the cluster to retrieve

        Returns:
            Dictionary with detailed cluster information or error message
        """
        try:
            client = get_workspace_client()
            cluster = client.clusters.get(cluster_id)
            
            return {
                'status': 'success',
                'cluster': {
                    'cluster_id': cluster.cluster_id,
                    'cluster_name': cluster.cluster_name,
                    'state': cluster.state.name if cluster.state else None,
                    'state_message': cluster.state_message,
                    'node_type_id': cluster.node_type_id,
                    'driver_node_type_id': cluster.driver_node_type_id,
                    'num_workers': cluster.num_workers,
                    'autoscale': cluster.autoscale.dict() if cluster.autoscale else None,
                    'spark_version': cluster.spark_version,
                    'spark_conf': cluster.spark_conf,
                    'aws_attributes': cluster.aws_attributes.dict() if cluster.aws_attributes else None,
                    'azure_attributes': cluster.azure_attributes.dict() if cluster.azure_attributes else None,
                    'gcp_attributes': cluster.gcp_attributes.dict() if cluster.gcp_attributes else None,
                    'cluster_source': cluster.cluster_source.name if cluster.cluster_source else None,
                    'creator_user_name': cluster.creator_user_name,
                    'start_time': cluster.start_time,
                    'terminated_time': cluster.terminated_time,
                    'last_state_loss_time': cluster.last_state_loss_time,
                    'last_activity_time': cluster.last_activity_time,
                    'cluster_memory_mb': cluster.cluster_memory_mb,
                    'cluster_cores': cluster.cluster_cores,
                    'default_tags': cluster.default_tags,
                    'custom_tags': cluster.custom_tags,
                    'init_scripts': [script.dict() for script in cluster.init_scripts] if cluster.init_scripts else None,
                    'enable_elastic_disk': cluster.enable_elastic_disk,
                    'disk_spec': cluster.disk_spec.dict() if cluster.disk_spec else None,
                    'cluster_log_conf': cluster.cluster_log_conf.dict() if cluster.cluster_log_conf else None
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def create_cluster(cluster_config: dict) -> dict:
        """Create a new cluster in the Databricks workspace.

        Args:
            cluster_config: Dictionary containing cluster configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            # Create cluster using the provided configuration
            response = client.clusters.create(**cluster_config)
            
            return {
                'status': 'success',
                'cluster_id': response.cluster_id,
                'message': f'Cluster {response.cluster_id} creation initiated'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def start_cluster(cluster_id: str) -> dict:
        """Start a terminated cluster.

        Args:
            cluster_id: The ID of the cluster to start

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.clusters.start(cluster_id)
            
            return {
                'status': 'success',
                'message': f'Cluster {cluster_id} start initiated'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def restart_cluster(cluster_id: str) -> dict:
        """Restart a cluster.

        Args:
            cluster_id: The ID of the cluster to restart

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.clusters.restart(cluster_id)
            
            return {
                'status': 'success',
                'message': f'Cluster {cluster_id} restart initiated'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def terminate_cluster(cluster_id: str) -> dict:
        """Terminate a running cluster.

        Args:
            cluster_id: The ID of the cluster to terminate

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.clusters.delete(cluster_id)
            
            return {
                'status': 'success',
                'message': f'Cluster {cluster_id} termination initiated'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_cluster(cluster_id: str) -> dict:
        """Permanently delete a cluster.

        Args:
            cluster_id: The ID of the cluster to delete

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.clusters.permanent_delete(cluster_id)
            
            return {
                'status': 'success',
                'message': f'Cluster {cluster_id} permanent deletion initiated'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def edit_cluster(cluster_id: str, cluster_config: dict) -> dict:
        """Edit an existing cluster configuration.

        Args:
            cluster_id: The ID of the cluster to edit
            cluster_config: Dictionary containing updated cluster configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            # Add cluster_id to config
            cluster_config['cluster_id'] = cluster_id
            
            client.clusters.edit(**cluster_config)
            
            return {
                'status': 'success',
                'message': f'Cluster {cluster_id} configuration updated'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_cluster_events(cluster_id: str, start_time: int = None, end_time: int = None, order: str = "DESC", limit: int = 50) -> dict:
        """Get events for a specific cluster.

        Args:
            cluster_id: The ID of the cluster to get events for
            start_time: The start time in epoch milliseconds (optional)
            end_time: The end time in epoch milliseconds (optional)
            order: Order of events (ASC or DESC, default: DESC)
            limit: Maximum number of events to return (default: 50)

        Returns:
            Dictionary with cluster events or error message
        """
        try:
            client = get_workspace_client()
            
            events_response = client.clusters.get_events(
                cluster_id=cluster_id,
                start_time=start_time,
                end_time=end_time,
                order=order,
                limit=limit
            )
            
            events_list = []
            if events_response.events:
                for event in events_response.events:
                    event_info = {
                        'cluster_id': event.cluster_id,
                        'timestamp': event.timestamp,
                        'type': event.type.name if event.type else None,
                        'details': event.details.dict() if event.details else None
                    }
                    events_list.append(event_info)
            
            return {
                'status': 'success',
                'events': events_list,
                'count': len(events_list),
                'next_page': events_response.next_page,
                'total_count': events_response.total_count
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Instance Pools API Tools

    @mcp_server.tool
    def list_instance_pools() -> dict:
        """List all instance pools in the Databricks workspace.

        Returns:
            Dictionary containing list of instance pools with their details
        """
        try:
            client = get_workspace_client()
            pools = list(client.instance_pools.list())
            
            pool_list = []
            for pool in pools:
                pool_info = {
                    'instance_pool_id': pool.instance_pool_id,
                    'instance_pool_name': pool.instance_pool_name,
                    'state': pool.state.name if pool.state else None,
                    'node_type_id': pool.node_type_id,
                    'min_idle_instances': pool.min_idle_instances,
                    'max_capacity': pool.max_capacity,
                    'idle_instance_autotermination_minutes': pool.idle_instance_autotermination_minutes,
                    'enable_elastic_disk': pool.enable_elastic_disk,
                    'disk_spec': pool.disk_spec.dict() if pool.disk_spec else None,
                    'preloaded_spark_versions': pool.preloaded_spark_versions,
                    'aws_attributes': pool.aws_attributes.dict() if pool.aws_attributes else None,
                    'azure_attributes': pool.azure_attributes.dict() if pool.azure_attributes else None,
                    'gcp_attributes': pool.gcp_attributes.dict() if pool.gcp_attributes else None,
                    'custom_tags': pool.custom_tags,
                    'default_tags': pool.default_tags,
                    'stats': pool.stats.dict() if pool.stats else None
                }
                pool_list.append(pool_info)

            return {
                'status': 'success',
                'instance_pools': pool_list,
                'count': len(pool_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_instance_pool(instance_pool_id: str) -> dict:
        """Get details of a specific instance pool.

        Args:
            instance_pool_id: The ID of the instance pool to get details for

        Returns:
            Dictionary with instance pool details or error message
        """
        try:
            client = get_workspace_client()
            pool = client.instance_pools.get(instance_pool_id)
            
            return {
                'status': 'success',
                'instance_pool': {
                    'instance_pool_id': pool.instance_pool_id,
                    'instance_pool_name': pool.instance_pool_name,
                    'state': pool.state.name if pool.state else None,
                    'node_type_id': pool.node_type_id,
                    'min_idle_instances': pool.min_idle_instances,
                    'max_capacity': pool.max_capacity,
                    'idle_instance_autotermination_minutes': pool.idle_instance_autotermination_minutes,
                    'enable_elastic_disk': pool.enable_elastic_disk,
                    'disk_spec': pool.disk_spec.dict() if pool.disk_spec else None,
                    'preloaded_spark_versions': pool.preloaded_spark_versions,
                    'preloaded_docker_images': [img.dict() for img in pool.preloaded_docker_images] if pool.preloaded_docker_images else None,
                    'aws_attributes': pool.aws_attributes.dict() if pool.aws_attributes else None,
                    'azure_attributes': pool.azure_attributes.dict() if pool.azure_attributes else None,
                    'gcp_attributes': pool.gcp_attributes.dict() if pool.gcp_attributes else None,
                    'custom_tags': pool.custom_tags,
                    'default_tags': pool.default_tags,
                    'stats': pool.stats.dict() if pool.stats else None
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def create_instance_pool(pool_config: dict) -> dict:
        """Create a new instance pool.

        Args:
            pool_config: Dictionary containing instance pool configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            response = client.instance_pools.create(**pool_config)
            
            return {
                'status': 'success',
                'instance_pool_id': response.instance_pool_id,
                'message': f'Instance pool {response.instance_pool_id} created successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def edit_instance_pool(instance_pool_id: str, pool_config: dict) -> dict:
        """Edit an existing instance pool.

        Args:
            instance_pool_id: The ID of the instance pool to edit
            pool_config: Dictionary containing updated pool configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            # Add instance_pool_id to config
            pool_config['instance_pool_id'] = instance_pool_id
            
            client.instance_pools.edit(**pool_config)
            
            return {
                'status': 'success',
                'message': f'Instance pool {instance_pool_id} updated successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_instance_pool(instance_pool_id: str) -> dict:
        """Delete an instance pool.

        Args:
            instance_pool_id: The ID of the instance pool to delete

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.instance_pools.delete(instance_pool_id)
            
            return {
                'status': 'success',
                'message': f'Instance pool {instance_pool_id} deleted successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Libraries API Tools

    @mcp_server.tool
    def list_cluster_libraries(cluster_id: str) -> dict:
        """List all libraries installed on a cluster.

        Args:
            cluster_id: The ID of the cluster to list libraries for

        Returns:
            Dictionary with library information or error message
        """
        try:
            client = get_workspace_client()
            libraries = client.libraries.cluster_status(cluster_id)
            
            library_list = []
            if libraries.library_statuses:
                for lib_status in libraries.library_statuses:
                    lib_info = {
                        'library': lib_status.library.dict() if lib_status.library else None,
                        'status': lib_status.status.name if lib_status.status else None,
                        'messages': lib_status.messages,
                        'is_library_for_all_clusters': lib_status.is_library_for_all_clusters
                    }
                    library_list.append(lib_info)
            
            return {
                'status': 'success',
                'cluster_id': libraries.cluster_id,
                'libraries': library_list,
                'count': len(library_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def install_cluster_libraries(cluster_id: str, libraries: list) -> dict:
        """Install libraries on a cluster.

        Args:
            cluster_id: The ID of the cluster to install libraries on
            libraries: List of library configurations to install

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            client.libraries.install(cluster_id=cluster_id, libraries=libraries)
            
            return {
                'status': 'success',
                'message': f'Libraries installation initiated on cluster {cluster_id}',
                'libraries_count': len(libraries)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def uninstall_cluster_libraries(cluster_id: str, libraries: list) -> dict:
        """Uninstall libraries from a cluster.

        Args:
            cluster_id: The ID of the cluster to uninstall libraries from
            libraries: List of library configurations to uninstall

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            client.libraries.uninstall(cluster_id=cluster_id, libraries=libraries)
            
            return {
                'status': 'success',
                'message': f'Libraries uninstallation initiated on cluster {cluster_id}',
                'libraries_count': len(libraries)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def list_all_cluster_libraries() -> dict:
        """List library status for all clusters.

        Returns:
            Dictionary with library status for all clusters or error message
        """
        try:
            client = get_workspace_client()
            all_statuses = list(client.libraries.all_cluster_statuses())
            
            cluster_libraries = []
            for status in all_statuses:
                cluster_info = {
                    'cluster_id': status.cluster_id,
                    'library_statuses': []
                }
                
                if status.library_statuses:
                    for lib_status in status.library_statuses:
                        lib_info = {
                            'library': lib_status.library.dict() if lib_status.library else None,
                            'status': lib_status.status.name if lib_status.status else None,
                            'messages': lib_status.messages,
                            'is_library_for_all_clusters': lib_status.is_library_for_all_clusters
                        }
                        cluster_info['library_statuses'].append(lib_info)
                
                cluster_libraries.append(cluster_info)
            
            return {
                'status': 'success',
                'cluster_libraries': cluster_libraries,
                'clusters_count': len(cluster_libraries)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Cluster Policies API Tools

    @mcp_server.tool
    def list_cluster_policies() -> dict:
        """List all cluster policies in the workspace.

        Returns:
            Dictionary containing list of cluster policies or error message
        """
        try:
            client = get_workspace_client()
            policies = list(client.cluster_policies.list())
            
            policy_list = []
            for policy in policies:
                policy_info = {
                    'policy_id': policy.policy_id,
                    'name': policy.name,
                    'definition': policy.definition,
                    'created_at_timestamp': policy.created_at_timestamp,
                    'is_default': policy.is_default,
                    'max_clusters_per_user': policy.max_clusters_per_user,
                    'policy_family_id': policy.policy_family_id,
                    'policy_family_definition_overrides': policy.policy_family_definition_overrides
                }
                policy_list.append(policy_info)

            return {
                'status': 'success',
                'policies': policy_list,
                'count': len(policy_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_cluster_policy(policy_id: str) -> dict:
        """Get details of a specific cluster policy.

        Args:
            policy_id: The ID of the cluster policy to retrieve

        Returns:
            Dictionary with cluster policy details or error message
        """
        try:
            client = get_workspace_client()
            policy = client.cluster_policies.get(policy_id)
            
            return {
                'status': 'success',
                'policy': {
                    'policy_id': policy.policy_id,
                    'name': policy.name,
                    'definition': policy.definition,
                    'created_at_timestamp': policy.created_at_timestamp,
                    'is_default': policy.is_default,
                    'max_clusters_per_user': policy.max_clusters_per_user,
                    'policy_family_id': policy.policy_family_id,
                    'policy_family_definition_overrides': policy.policy_family_definition_overrides
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def create_cluster_policy(policy_config: dict) -> dict:
        """Create a new cluster policy.

        Args:
            policy_config: Dictionary containing cluster policy configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            response = client.cluster_policies.create(**policy_config)
            
            return {
                'status': 'success',
                'policy_id': response.policy_id,
                'message': f'Cluster policy {response.policy_id} created successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def edit_cluster_policy(policy_id: str, policy_config: dict) -> dict:
        """Edit an existing cluster policy.

        Args:
            policy_id: The ID of the cluster policy to edit
            policy_config: Dictionary containing updated policy configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            # Add policy_id to config
            policy_config['policy_id'] = policy_id
            
            client.cluster_policies.edit(**policy_config)
            
            return {
                'status': 'success',
                'message': f'Cluster policy {policy_id} updated successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_cluster_policy(policy_id: str) -> dict:
        """Delete a cluster policy.

        Args:
            policy_id: The ID of the cluster policy to delete

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.cluster_policies.delete(policy_id)
            
            return {
                'status': 'success',
                'message': f'Cluster policy {policy_id} deleted successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Node Types and Spark Versions

    @mcp_server.tool
    def list_node_types() -> dict:
        """List available node types for clusters.

        Returns:
            Dictionary containing list of available node types or error message
        """
        try:
            client = get_workspace_client()
            node_types = client.clusters.list_node_types()
            
            node_types_list = []
            if node_types.node_types:
                for node_type in node_types.node_types:
                    node_info = {
                        'node_type_id': node_type.node_type_id,
                        'memory_mb': node_type.memory_mb,
                        'num_cores': node_type.num_cores,
                        'description': node_type.description,
                        'instance_type_id': node_type.instance_type_id,
                        'is_deprecated': node_type.is_deprecated,
                        'category': node_type.category,
                        'support_ebs_volumes': node_type.support_ebs_volumes,
                        'support_cluster_tags': node_type.support_cluster_tags,
                        'support_port_forwarding': node_type.support_port_forwarding,
                        'display_order': node_type.display_order,
                        'node_info': node_type.node_info.dict() if node_type.node_info else None
                    }
                    node_types_list.append(node_info)
            
            return {
                'status': 'success',
                'node_types': node_types_list,
                'count': len(node_types_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def list_spark_versions() -> dict:
        """List available Spark versions for clusters.

        Returns:
            Dictionary containing list of available Spark versions or error message
        """
        try:
            client = get_workspace_client()
            spark_versions = client.clusters.spark_versions()
            
            versions_list = []
            if spark_versions.versions:
                for version in spark_versions.versions:
                    version_info = {
                        'key': version.key,
                        'name': version.name
                    }
                    versions_list.append(version_info)
            
            return {
                'status': 'success',
                'spark_versions': versions_list,
                'count': len(versions_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}