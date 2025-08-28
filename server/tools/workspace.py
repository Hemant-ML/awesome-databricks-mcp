"""Workspace and repository management MCP tools for Databricks."""

from databricks.sdk import WorkspaceClient


def load_workspace_tools(mcp_server):
    """Register workspace and repository MCP tools with the server.

    Args:
        mcp_server: The FastMCP server instance to register tools with
    """

    def get_workspace_client():
        """Get authenticated Databricks workspace client."""
        return WorkspaceClient()

    # Workspace API Tools

    @mcp_server.tool
    def list_workspace_objects(path: str = "/") -> dict:
        """List objects in the workspace directory.

        Args:
            path: Workspace path to list (default: "/")

        Returns:
            Dictionary containing list of workspace objects or error message
        """
        try:
            client = get_workspace_client()
            objects = list(client.workspace.list(path))
            
            object_list = []
            for obj in objects:
                object_info = {
                    'object_type': obj.object_type.name if obj.object_type else None,
                    'path': obj.path,
                    'language': obj.language.name if obj.language else None,
                    'object_id': obj.object_id,
                    'size': obj.size,
                    'created_at': obj.created_at,
                    'modified_at': obj.modified_at
                }
                object_list.append(object_info)

            return {
                'status': 'success',
                'path': path,
                'objects': object_list,
                'count': len(object_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_workspace_object(path: str) -> dict:
        """Get status/information about a workspace object.

        Args:
            path: Workspace path to get information for

        Returns:
            Dictionary with workspace object details or error message
        """
        try:
            client = get_workspace_client()
            obj = client.workspace.get_status(path)
            
            return {
                'status': 'success',
                'object': {
                    'object_type': obj.object_type.name if obj.object_type else None,
                    'path': obj.path,
                    'language': obj.language.name if obj.language else None,
                    'object_id': obj.object_id,
                    'size': obj.size,
                    'created_at': obj.created_at,
                    'modified_at': obj.modified_at
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def export_workspace_object(path: str, format: str = "SOURCE") -> dict:
        """Export a workspace object.

        Args:
            path: Workspace path to export
            format: Export format (SOURCE, HTML, JUPYTER, DBC)

        Returns:
            Dictionary with exported content or error message
        """
        try:
            client = get_workspace_client()
            
            # Export the object
            export_response = client.workspace.export(path, format=format)
            
            # Convert bytes to string if possible
            try:
                if format == "SOURCE":
                    content = export_response.content.decode('utf-8')
                    content_type = 'text'
                else:
                    content = str(export_response.content)
                    content_type = 'binary'
            except UnicodeDecodeError:
                content = str(export_response.content)
                content_type = 'binary'

            return {
                'status': 'success',
                'path': path,
                'format': format,
                'content': content,
                'content_type': content_type,
                'size': len(export_response.content)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def import_workspace_object(path: str, content: str, language: str = "PYTHON", format: str = "SOURCE", overwrite: bool = False) -> dict:
        """Import content to a workspace object.

        Args:
            path: Workspace path to import to
            content: Content to import
            language: Programming language (PYTHON, SCALA, SQL, R)
            format: Import format (SOURCE, HTML, JUPYTER, DBC)
            overwrite: Whether to overwrite existing object

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            # Convert string to bytes
            content_bytes = content.encode('utf-8')
            
            # Import the content
            client.workspace.import_(
                path=path,
                content=content_bytes,
                language=language,
                format=format,
                overwrite=overwrite
            )
            
            return {
                'status': 'success',
                'path': path,
                'language': language,
                'format': format,
                'overwrite': overwrite,
                'content_size': len(content_bytes),
                'message': f'Content imported successfully to {path}'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_workspace_object(path: str, recursive: bool = False) -> dict:
        """Delete a workspace object.

        Args:
            path: Workspace path to delete
            recursive: Whether to delete directory contents recursively

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.workspace.delete(path, recursive=recursive)
            
            return {
                'status': 'success',
                'path': path,
                'recursive': recursive,
                'message': f'Workspace object {path} deleted successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def create_workspace_directory(path: str) -> dict:
        """Create a directory in the workspace.

        Args:
            path: Workspace directory path to create

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.workspace.mkdirs(path)
            
            return {
                'status': 'success',
                'path': path,
                'message': f'Workspace directory {path} created successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Git Repositories API Tools

    @mcp_server.tool
    def list_repos() -> dict:
        """List all Git repositories in the workspace.

        Returns:
            Dictionary containing list of repositories or error message
        """
        try:
            client = get_workspace_client()
            repos = list(client.repos.list())
            
            repo_list = []
            for repo in repos:
                repo_info = {
                    'id': repo.id,
                    'path': repo.path,
                    'url': repo.url,
                    'provider': repo.provider,
                    'branch': repo.branch,
                    'head_commit_id': repo.head_commit_id
                }
                repo_list.append(repo_info)

            return {
                'status': 'success',
                'repositories': repo_list,
                'count': len(repo_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_repo(repo_id: str) -> dict:
        """Get details of a specific Git repository.

        Args:
            repo_id: The ID of the repository to retrieve

        Returns:
            Dictionary with repository details or error message
        """
        try:
            client = get_workspace_client()
            repo = client.repos.get(repo_id)
            
            return {
                'status': 'success',
                'repository': {
                    'id': repo.id,
                    'path': repo.path,
                    'url': repo.url,
                    'provider': repo.provider,
                    'branch': repo.branch,
                    'head_commit_id': repo.head_commit_id
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def create_repo(repo_config: dict) -> dict:
        """Clone a Git repository into the workspace.

        Args:
            repo_config: Dictionary containing repository configuration
                        (url, provider, path, branch)

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            repo = client.repos.create(**repo_config)
            
            return {
                'status': 'success',
                'repository_id': repo.id,
                'path': repo.path,
                'url': repo.url,
                'branch': repo.branch,
                'message': f'Repository cloned successfully to {repo.path}'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def update_repo(repo_id: str, branch: str = None, tag: str = None) -> dict:
        """Update a Git repository to a specific branch or tag.

        Args:
            repo_id: The ID of the repository to update
            branch: Branch name to checkout (optional)
            tag: Tag name to checkout (optional)

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            update_config = {}
            if branch:
                update_config['branch'] = branch
            if tag:
                update_config['tag'] = tag
                
            client.repos.update(repo_id, **update_config)
            
            return {
                'status': 'success',
                'repository_id': repo_id,
                'branch': branch,
                'tag': tag,
                'message': f'Repository {repo_id} updated successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_repo(repo_id: str) -> dict:
        """Delete a Git repository from the workspace.

        Args:
            repo_id: The ID of the repository to delete

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.repos.delete(repo_id)
            
            return {
                'status': 'success',
                'repository_id': repo_id,
                'message': f'Repository {repo_id} deleted successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_repo_permissions(repo_id: str) -> dict:
        """Get permissions for a Git repository.

        Args:
            repo_id: The ID of the repository to get permissions for

        Returns:
            Dictionary with repository permissions or error message
        """
        try:
            client = get_workspace_client()
            permissions = client.repo_permissions.get(repo_id)
            
            permission_list = []
            if permissions.access_control_list:
                for acl in permissions.access_control_list:
                    permission_info = {
                        'user_name': acl.user_name,
                        'group_name': acl.group_name,
                        'service_principal_name': acl.service_principal_name,
                        'all_permissions': [perm.dict() for perm in acl.all_permissions] if acl.all_permissions else None
                    }
                    permission_list.append(permission_info)
            
            return {
                'status': 'success',
                'repository_id': repo_id,
                'object_id': permissions.object_id,
                'object_type': permissions.object_type,
                'permissions': permission_list,
                'count': len(permission_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def set_repo_permissions(repo_id: str, access_control_list: list) -> dict:
        """Set permissions for a Git repository.

        Args:
            repo_id: The ID of the repository to set permissions for
            access_control_list: List of access control entries

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.repo_permissions.set(repo_id, access_control_list=access_control_list)
            
            return {
                'status': 'success',
                'repository_id': repo_id,
                'permissions_count': len(access_control_list),
                'message': f'Permissions set successfully for repository {repo_id}'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def update_repo_permissions(repo_id: str, access_control_list: list) -> dict:
        """Update permissions for a Git repository.

        Args:
            repo_id: The ID of the repository to update permissions for
            access_control_list: List of access control entries to update

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.repo_permissions.update(repo_id, access_control_list=access_control_list)
            
            return {
                'status': 'success',
                'repository_id': repo_id,
                'permissions_count': len(access_control_list),
                'message': f'Permissions updated successfully for repository {repo_id}'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Workspace Permissions API Tools

    @mcp_server.tool
    def get_workspace_permissions(workspace_object_type: str, workspace_object_id: str) -> dict:
        """Get permissions for a workspace object.

        Args:
            workspace_object_type: Type of workspace object
            workspace_object_id: ID of the workspace object

        Returns:
            Dictionary with workspace object permissions or error message
        """
        try:
            client = get_workspace_client()
            permissions = client.permissions.get(workspace_object_type, workspace_object_id)
            
            permission_list = []
            if permissions.access_control_list:
                for acl in permissions.access_control_list:
                    permission_info = {
                        'user_name': acl.user_name,
                        'group_name': acl.group_name,
                        'service_principal_name': acl.service_principal_name,
                        'all_permissions': [perm.dict() for perm in acl.all_permissions] if acl.all_permissions else None
                    }
                    permission_list.append(permission_info)
            
            return {
                'status': 'success',
                'object_type': workspace_object_type,
                'object_id': workspace_object_id,
                'permissions': permission_list,
                'count': len(permission_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def set_workspace_permissions(workspace_object_type: str, workspace_object_id: str, access_control_list: list) -> dict:
        """Set permissions for a workspace object.

        Args:
            workspace_object_type: Type of workspace object
            workspace_object_id: ID of the workspace object
            access_control_list: List of access control entries

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.permissions.set(workspace_object_type, workspace_object_id, access_control_list=access_control_list)
            
            return {
                'status': 'success',
                'object_type': workspace_object_type,
                'object_id': workspace_object_id,
                'permissions_count': len(access_control_list),
                'message': f'Permissions set successfully for {workspace_object_type} {workspace_object_id}'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def update_workspace_permissions(workspace_object_type: str, workspace_object_id: str, access_control_list: list) -> dict:
        """Update permissions for a workspace object.

        Args:
            workspace_object_type: Type of workspace object
            workspace_object_id: ID of the workspace object
            access_control_list: List of access control entries to update

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.permissions.update(workspace_object_type, workspace_object_id, access_control_list=access_control_list)
            
            return {
                'status': 'success',
                'object_type': workspace_object_type,
                'object_id': workspace_object_id,
                'permissions_count': len(access_control_list),
                'message': f'Permissions updated successfully for {workspace_object_type} {workspace_object_id}'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Current User API Tools

    @mcp_server.tool
    def get_current_user() -> dict:
        """Get information about the current user.

        Returns:
            Dictionary with current user details or error message
        """
        try:
            client = get_workspace_client()
            user = client.current_user.me()
            
            return {
                'status': 'success',
                'current_user': {
                    'id': user.id,
                    'user_name': user.user_name,
                    'display_name': user.display_name,
                    'active': user.active,
                    'emails': [email.dict() for email in user.emails] if user.emails else None,
                    'groups': [group.dict() for group in user.groups] if user.groups else None,
                    'roles': [role.dict() for role in user.roles] if user.roles else None
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Workspace Settings API Tools

    @mcp_server.tool
    def get_workspace_settings() -> dict:
        """Get workspace-wide settings.

        Returns:
            Dictionary with workspace settings or error message
        """
        try:
            client = get_workspace_client()
            
            # Get various workspace settings
            settings = {}
            
            # Try to get token management settings
            try:
                token_settings = client.settings.get_token_management()
                settings['token_management'] = token_settings.dict() if token_settings else None
            except:
                settings['token_management'] = None
                
            # Try to get IP access list settings
            try:
                ip_settings = client.settings.get_ip_access_list()
                settings['ip_access_list'] = ip_settings.dict() if ip_settings else None
            except:
                settings['ip_access_list'] = None
            
            return {
                'status': 'success',
                'workspace_settings': settings
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def update_token_management_settings(token_config: dict) -> dict:
        """Update token management settings.

        Args:
            token_config: Dictionary containing token management configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.settings.update_token_management(**token_config)
            
            return {
                'status': 'success',
                'message': 'Token management settings updated successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def update_ip_access_list_settings(ip_config: dict) -> dict:
        """Update IP access list settings.

        Args:
            ip_config: Dictionary containing IP access list configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.settings.update_ip_access_list(**ip_config)
            
            return {
                'status': 'success',
                'message': 'IP access list settings updated successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}