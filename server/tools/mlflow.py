"""MLflow and ML model management MCP tools for Databricks."""

from databricks.sdk import WorkspaceClient


def load_mlflow_tools(mcp_server):
    """Register MLflow and ML model management MCP tools with the server.

    Args:
        mcp_server: The FastMCP server instance to register tools with
    """

    def get_workspace_client():
        """Get authenticated Databricks workspace client."""
        return WorkspaceClient()

    # Model Registry API Tools

    @mcp_server.tool
    def list_models() -> dict:
        """List all registered models in the model registry.

        Returns:
            Dictionary containing list of registered models or error message
        """
        try:
            client = get_workspace_client()
            models = list(client.model_registry.list_models())
            
            model_list = []
            for model in models:
                model_info = {
                    'name': model.name,
                    'creation_timestamp': model.creation_timestamp,
                    'last_updated_timestamp': model.last_updated_timestamp,
                    'user_id': model.user_id,
                    'description': model.description,
                    'latest_versions': [version.dict() for version in model.latest_versions] if model.latest_versions else None,
                    'tags': [tag.dict() for tag in model.tags] if model.tags else None
                }
                model_list.append(model_info)

            return {
                'status': 'success',
                'models': model_list,
                'count': len(model_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_model(model_name: str) -> dict:
        """Get details of a specific registered model.

        Args:
            model_name: Name of the model to retrieve

        Returns:
            Dictionary with model details or error message
        """
        try:
            client = get_workspace_client()
            model = client.model_registry.get_model(model_name)
            
            return {
                'status': 'success',
                'model': {
                    'name': model.name,
                    'creation_timestamp': model.creation_timestamp,
                    'last_updated_timestamp': model.last_updated_timestamp,
                    'user_id': model.user_id,
                    'description': model.description,
                    'latest_versions': [version.dict() for version in model.latest_versions] if model.latest_versions else None,
                    'tags': [tag.dict() for tag in model.tags] if model.tags else None
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def create_model(model_name: str, description: str = None, tags: list = None) -> dict:
        """Create a new registered model.

        Args:
            model_name: Name of the model to create
            description: Optional description for the model
            tags: Optional list of tags for the model

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            create_config = {'name': model_name}
            if description:
                create_config['description'] = description
            if tags:
                create_config['tags'] = tags
                
            model = client.model_registry.create_model(**create_config)
            
            return {
                'status': 'success',
                'model_name': model.name,
                'creation_timestamp': model.creation_timestamp,
                'message': f'Model {model_name} created successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def update_model(model_name: str, description: str = None) -> dict:
        """Update a registered model.

        Args:
            model_name: Name of the model to update
            description: Updated description for the model

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            update_config = {'name': model_name}
            if description:
                update_config['description'] = description
                
            client.model_registry.update_model(**update_config)
            
            return {
                'status': 'success',
                'model_name': model_name,
                'message': f'Model {model_name} updated successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_model(model_name: str) -> dict:
        """Delete a registered model.

        Args:
            model_name: Name of the model to delete

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.model_registry.delete_model(model_name)
            
            return {
                'status': 'success',
                'model_name': model_name,
                'message': f'Model {model_name} deleted successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Model Versions API Tools

    @mcp_server.tool
    def list_model_versions(model_name: str) -> dict:
        """List all versions of a specific model.

        Args:
            model_name: Name of the model to list versions for

        Returns:
            Dictionary containing list of model versions or error message
        """
        try:
            client = get_workspace_client()
            versions = list(client.model_registry.list_model_versions(model_name))
            
            version_list = []
            for version in versions:
                version_info = {
                    'name': version.name,
                    'version': version.version,
                    'creation_timestamp': version.creation_timestamp,
                    'last_updated_timestamp': version.last_updated_timestamp,
                    'user_id': version.user_id,
                    'description': version.description,
                    'source': version.source,
                    'run_id': version.run_id,
                    'status': version.status,
                    'status_message': version.status_message,
                    'current_stage': version.current_stage,
                    'tags': [tag.dict() for tag in version.tags] if version.tags else None
                }
                version_list.append(version_info)

            return {
                'status': 'success',
                'model_name': model_name,
                'versions': version_list,
                'count': len(version_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_model_version(model_name: str, version: str) -> dict:
        """Get details of a specific model version.

        Args:
            model_name: Name of the model
            version: Version number to retrieve

        Returns:
            Dictionary with model version details or error message
        """
        try:
            client = get_workspace_client()
            model_version = client.model_registry.get_model_version(model_name, version)
            
            return {
                'status': 'success',
                'model_version': {
                    'name': model_version.name,
                    'version': model_version.version,
                    'creation_timestamp': model_version.creation_timestamp,
                    'last_updated_timestamp': model_version.last_updated_timestamp,
                    'user_id': model_version.user_id,
                    'description': model_version.description,
                    'source': model_version.source,
                    'run_id': model_version.run_id,
                    'status': model_version.status,
                    'status_message': model_version.status_message,
                    'current_stage': model_version.current_stage,
                    'tags': [tag.dict() for tag in model_version.tags] if model_version.tags else None
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def create_model_version(model_name: str, source: str, run_id: str = None, description: str = None, tags: list = None) -> dict:
        """Create a new model version.

        Args:
            model_name: Name of the model
            source: URI of the model artifacts
            run_id: Optional MLflow run ID
            description: Optional description for the version
            tags: Optional list of tags for the version

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            create_config = {
                'name': model_name,
                'source': source
            }
            if run_id:
                create_config['run_id'] = run_id
            if description:
                create_config['description'] = description
            if tags:
                create_config['tags'] = tags
                
            version = client.model_registry.create_model_version(**create_config)
            
            return {
                'status': 'success',
                'model_name': model_name,
                'version': version.version,
                'source': source,
                'message': f'Model version {version.version} created successfully for {model_name}'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def update_model_version(model_name: str, version: str, description: str = None, stage: str = None) -> dict:
        """Update a model version.

        Args:
            model_name: Name of the model
            version: Version number to update
            description: Updated description for the version
            stage: Stage to transition to (None, Staging, Production, Archived)

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            update_config = {
                'name': model_name,
                'version': version
            }
            if description:
                update_config['description'] = description
                
            client.model_registry.update_model_version(**update_config)
            
            # Handle stage transition separately if specified
            if stage:
                client.model_registry.transition_model_version_stage(
                    name=model_name,
                    version=version,
                    stage=stage
                )
            
            return {
                'status': 'success',
                'model_name': model_name,
                'version': version,
                'stage': stage,
                'message': f'Model version {version} updated successfully for {model_name}'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_model_version(model_name: str, version: str) -> dict:
        """Delete a model version.

        Args:
            model_name: Name of the model
            version: Version number to delete

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.model_registry.delete_model_version(model_name, version)
            
            return {
                'status': 'success',
                'model_name': model_name,
                'version': version,
                'message': f'Model version {version} deleted successfully from {model_name}'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def transition_model_version_stage(model_name: str, version: str, stage: str, archive_existing_versions: bool = False) -> dict:
        """Transition a model version to a different stage.

        Args:
            model_name: Name of the model
            version: Version number to transition
            stage: Target stage (None, Staging, Production, Archived)
            archive_existing_versions: Whether to archive existing versions in target stage

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            transition_config = {
                'name': model_name,
                'version': version,
                'stage': stage,
                'archive_existing_versions': archive_existing_versions
            }
            
            version_result = client.model_registry.transition_model_version_stage(**transition_config)
            
            return {
                'status': 'success',
                'model_name': model_name,
                'version': version,
                'stage': stage,
                'current_stage': version_result.current_stage,
                'message': f'Model version {version} transitioned to {stage} stage for {model_name}'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # MLflow Experiments API Tools

    @mcp_server.tool
    def list_experiments() -> dict:
        """List all MLflow experiments.

        Returns:
            Dictionary containing list of experiments or error message
        """
        try:
            client = get_workspace_client()
            experiments = list(client.experiments.list_experiments())
            
            experiment_list = []
            for experiment in experiments:
                experiment_info = {
                    'experiment_id': experiment.experiment_id,
                    'name': experiment.name,
                    'artifact_location': experiment.artifact_location,
                    'lifecycle_stage': experiment.lifecycle_stage,
                    'last_update_time': experiment.last_update_time,
                    'creation_time': experiment.creation_time,
                    'tags': [tag.dict() for tag in experiment.tags] if experiment.tags else None
                }
                experiment_list.append(experiment_info)

            return {
                'status': 'success',
                'experiments': experiment_list,
                'count': len(experiment_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_experiment(experiment_id: str) -> dict:
        """Get details of a specific MLflow experiment.

        Args:
            experiment_id: ID of the experiment to retrieve

        Returns:
            Dictionary with experiment details or error message
        """
        try:
            client = get_workspace_client()
            experiment = client.experiments.get_experiment(experiment_id)
            
            return {
                'status': 'success',
                'experiment': {
                    'experiment_id': experiment.experiment_id,
                    'name': experiment.name,
                    'artifact_location': experiment.artifact_location,
                    'lifecycle_stage': experiment.lifecycle_stage,
                    'last_update_time': experiment.last_update_time,
                    'creation_time': experiment.creation_time,
                    'tags': [tag.dict() for tag in experiment.tags] if experiment.tags else None
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def create_experiment(name: str, artifact_location: str = None, tags: list = None) -> dict:
        """Create a new MLflow experiment.

        Args:
            name: Name of the experiment
            artifact_location: Optional artifact storage location
            tags: Optional list of tags for the experiment

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            create_config = {'name': name}
            if artifact_location:
                create_config['artifact_location'] = artifact_location
            if tags:
                create_config['tags'] = tags
                
            experiment_id = client.experiments.create_experiment(**create_config)
            
            return {
                'status': 'success',
                'experiment_id': experiment_id,
                'name': name,
                'message': f'Experiment {name} created successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def update_experiment(experiment_id: str, new_name: str = None) -> dict:
        """Update an MLflow experiment.

        Args:
            experiment_id: ID of the experiment to update
            new_name: New name for the experiment

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            update_config = {'experiment_id': experiment_id}
            if new_name:
                update_config['new_name'] = new_name
                
            client.experiments.update_experiment(**update_config)
            
            return {
                'status': 'success',
                'experiment_id': experiment_id,
                'new_name': new_name,
                'message': f'Experiment {experiment_id} updated successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_experiment(experiment_id: str) -> dict:
        """Delete an MLflow experiment.

        Args:
            experiment_id: ID of the experiment to delete

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.experiments.delete_experiment(experiment_id)
            
            return {
                'status': 'success',
                'experiment_id': experiment_id,
                'message': f'Experiment {experiment_id} deleted successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def restore_experiment(experiment_id: str) -> dict:
        """Restore a deleted MLflow experiment.

        Args:
            experiment_id: ID of the experiment to restore

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.experiments.restore_experiment(experiment_id)
            
            return {
                'status': 'success',
                'experiment_id': experiment_id,
                'message': f'Experiment {experiment_id} restored successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # MLflow Runs API Tools

    @mcp_server.tool
    def search_runs(experiment_ids: list, filter_string: str = None, run_view_type: str = "ACTIVE_ONLY", max_results: int = 100) -> dict:
        """Search for MLflow runs.

        Args:
            experiment_ids: List of experiment IDs to search in
            filter_string: Optional filter string for runs
            run_view_type: Type of runs to return (ACTIVE_ONLY, DELETED_ONLY, ALL)
            max_results: Maximum number of results to return

        Returns:
            Dictionary containing list of runs or error message
        """
        try:
            client = get_workspace_client()
            
            search_config = {
                'experiment_ids': experiment_ids,
                'max_results': max_results
            }
            if filter_string:
                search_config['filter'] = filter_string
            if run_view_type:
                search_config['run_view_type'] = run_view_type
                
            runs_response = client.experiments.search_runs(**search_config)
            
            run_list = []
            if runs_response.runs:
                for run in runs_response.runs:
                    run_info = {
                        'run_id': run.info.run_id if run.info else None,
                        'run_uuid': run.info.run_uuid if run.info else None,
                        'experiment_id': run.info.experiment_id if run.info else None,
                        'user_id': run.info.user_id if run.info else None,
                        'status': run.info.status if run.info else None,
                        'start_time': run.info.start_time if run.info else None,
                        'end_time': run.info.end_time if run.info else None,
                        'artifact_uri': run.info.artifact_uri if run.info else None,
                        'lifecycle_stage': run.info.lifecycle_stage if run.info else None,
                        'run_name': run.info.run_name if run.info else None,
                        'tags': run.data.tags if run.data else None,
                        'params': run.data.params if run.data else None,
                        'metrics': run.data.metrics if run.data else None
                    }
                    run_list.append(run_info)

            return {
                'status': 'success',
                'experiment_ids': experiment_ids,
                'runs': run_list,
                'count': len(run_list),
                'next_page_token': runs_response.next_page_token if hasattr(runs_response, 'next_page_token') else None
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_run(run_id: str) -> dict:
        """Get details of a specific MLflow run.

        Args:
            run_id: ID of the run to retrieve

        Returns:
            Dictionary with run details or error message
        """
        try:
            client = get_workspace_client()
            run = client.experiments.get_run(run_id)
            
            return {
                'status': 'success',
                'run': {
                    'run_id': run.info.run_id if run.info else None,
                    'run_uuid': run.info.run_uuid if run.info else None,
                    'experiment_id': run.info.experiment_id if run.info else None,
                    'user_id': run.info.user_id if run.info else None,
                    'status': run.info.status if run.info else None,
                    'start_time': run.info.start_time if run.info else None,
                    'end_time': run.info.end_time if run.info else None,
                    'artifact_uri': run.info.artifact_uri if run.info else None,
                    'lifecycle_stage': run.info.lifecycle_stage if run.info else None,
                    'run_name': run.info.run_name if run.info else None,
                    'tags': run.data.tags if run.data else None,
                    'params': run.data.params if run.data else None,
                    'metrics': run.data.metrics if run.data else None
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_run(run_id: str) -> dict:
        """Delete an MLflow run.

        Args:
            run_id: ID of the run to delete

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.experiments.delete_run(run_id)
            
            return {
                'status': 'success',
                'run_id': run_id,
                'message': f'Run {run_id} deleted successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def restore_run(run_id: str) -> dict:
        """Restore a deleted MLflow run.

        Args:
            run_id: ID of the run to restore

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.experiments.restore_run(run_id)
            
            return {
                'status': 'success',
                'run_id': run_id,
                'message': f'Run {run_id} restored successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}