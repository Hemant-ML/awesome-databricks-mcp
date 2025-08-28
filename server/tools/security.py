"""Security and identity management MCP tools for Databricks."""

from databricks.sdk import WorkspaceClient


def load_security_tools(mcp_server):
    """Register security and identity MCP tools with the server.

    Args:
        mcp_server: The FastMCP server instance to register tools with
    """

    def get_workspace_client():
        """Get authenticated Databricks workspace client."""
        return WorkspaceClient()

    # Secrets API Tools

    @mcp_server.tool
    def list_secret_scopes() -> dict:
        """List all secret scopes in the workspace.

        Returns:
            Dictionary containing list of secret scopes or error message
        """
        try:
            client = get_workspace_client()
            scopes = list(client.secrets.list_scopes())
            
            scope_list = []
            for scope in scopes:
                scope_info = {
                    'name': scope.name,
                    'backend_type': scope.backend_type.name if scope.backend_type else None
                }
                scope_list.append(scope_info)

            return {
                'status': 'success',
                'scopes': scope_list,
                'count': len(scope_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def create_secret_scope(scope_name: str, backend_type: str = "DATABRICKS") -> dict:
        """Create a new secret scope.

        Args:
            scope_name: Name of the secret scope to create
            backend_type: Backend type for the scope (DATABRICKS or AZURE_KEYVAULT)

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.secrets.create_scope(scope=scope_name, backend_type=backend_type)
            
            return {
                'status': 'success',
                'scope_name': scope_name,
                'backend_type': backend_type,
                'message': f'Secret scope {scope_name} created successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_secret_scope(scope_name: str) -> dict:
        """Delete a secret scope.

        Args:
            scope_name: Name of the secret scope to delete

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.secrets.delete_scope(scope=scope_name)
            
            return {
                'status': 'success',
                'scope_name': scope_name,
                'message': f'Secret scope {scope_name} deleted successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def list_secrets(scope_name: str) -> dict:
        """List all secrets in a scope.

        Args:
            scope_name: Name of the secret scope to list secrets from

        Returns:
            Dictionary containing list of secrets or error message
        """
        try:
            client = get_workspace_client()
            secrets = list(client.secrets.list_secrets(scope=scope_name))
            
            secret_list = []
            for secret in secrets:
                secret_info = {
                    'key': secret.key,
                    'last_updated_timestamp': secret.last_updated_timestamp
                }
                secret_list.append(secret_info)

            return {
                'status': 'success',
                'scope_name': scope_name,
                'secrets': secret_list,
                'count': len(secret_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def put_secret(scope_name: str, key: str, value: str) -> dict:
        """Create or update a secret.

        Args:
            scope_name: Name of the secret scope
            key: Secret key name
            value: Secret value

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.secrets.put_secret(scope=scope_name, key=key, string_value=value)
            
            return {
                'status': 'success',
                'scope_name': scope_name,
                'key': key,
                'message': f'Secret {key} created/updated successfully in scope {scope_name}'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_secret(scope_name: str, key: str) -> dict:
        """Delete a secret.

        Args:
            scope_name: Name of the secret scope
            key: Secret key name to delete

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.secrets.delete_secret(scope=scope_name, key=key)
            
            return {
                'status': 'success',
                'scope_name': scope_name,
                'key': key,
                'message': f'Secret {key} deleted successfully from scope {scope_name}'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_secret_acl(scope_name: str, principal: str) -> dict:
        """Get ACL for a secret scope.

        Args:
            scope_name: Name of the secret scope
            principal: Principal (user or group) to get ACL for

        Returns:
            Dictionary with ACL information or error message
        """
        try:
            client = get_workspace_client()
            acl = client.secrets.get_acl(scope=scope_name, principal=principal)
            
            return {
                'status': 'success',
                'scope_name': scope_name,
                'principal': principal,
                'permission': acl.permission.name if acl.permission else None
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def list_secret_acls(scope_name: str) -> dict:
        """List all ACLs for a secret scope.

        Args:
            scope_name: Name of the secret scope

        Returns:
            Dictionary containing list of ACLs or error message
        """
        try:
            client = get_workspace_client()
            acls = list(client.secrets.list_acls(scope=scope_name))
            
            acl_list = []
            for acl in acls:
                acl_info = {
                    'principal': acl.principal,
                    'permission': acl.permission.name if acl.permission else None
                }
                acl_list.append(acl_info)

            return {
                'status': 'success',
                'scope_name': scope_name,
                'acls': acl_list,
                'count': len(acl_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def put_secret_acl(scope_name: str, principal: str, permission: str) -> dict:
        """Create or update ACL for a secret scope.

        Args:
            scope_name: Name of the secret scope
            principal: Principal (user or group) to set ACL for
            permission: Permission level (READ, WRITE, MANAGE)

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.secrets.put_acl(scope=scope_name, principal=principal, permission=permission)
            
            return {
                'status': 'success',
                'scope_name': scope_name,
                'principal': principal,
                'permission': permission,
                'message': f'ACL for {principal} set to {permission} on scope {scope_name}'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_secret_acl(scope_name: str, principal: str) -> dict:
        """Delete ACL for a secret scope.

        Args:
            scope_name: Name of the secret scope
            principal: Principal (user or group) to remove ACL for

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.secrets.delete_acl(scope=scope_name, principal=principal)
            
            return {
                'status': 'success',
                'scope_name': scope_name,
                'principal': principal,
                'message': f'ACL for {principal} deleted from scope {scope_name}'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Users API Tools

    @mcp_server.tool
    def list_users() -> dict:
        """List all users in the workspace.

        Returns:
            Dictionary containing list of users or error message
        """
        try:
            client = get_workspace_client()
            users = list(client.users.list())
            
            user_list = []
            for user in users:
                user_info = {
                    'id': user.id,
                    'user_name': user.user_name,
                    'display_name': user.display_name,
                    'active': user.active,
                    'emails': [email.dict() for email in user.emails] if user.emails else None,
                    'groups': [group.dict() for group in user.groups] if user.groups else None
                }
                user_list.append(user_info)

            return {
                'status': 'success',
                'users': user_list,
                'count': len(user_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_user(user_id: str) -> dict:
        """Get details of a specific user.

        Args:
            user_id: The ID of the user to retrieve

        Returns:
            Dictionary with user details or error message
        """
        try:
            client = get_workspace_client()
            user = client.users.get(user_id)
            
            return {
                'status': 'success',
                'user': {
                    'id': user.id,
                    'user_name': user.user_name,
                    'display_name': user.display_name,
                    'active': user.active,
                    'emails': [email.dict() for email in user.emails] if user.emails else None,
                    'groups': [group.dict() for group in user.groups] if user.groups else None,
                    'roles': [role.dict() for role in user.roles] if user.roles else None,
                    'schemas': user.schemas
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def create_user(user_config: dict) -> dict:
        """Create a new user in the workspace.

        Args:
            user_config: Dictionary containing user configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            user = client.users.create(**user_config)
            
            return {
                'status': 'success',
                'user_id': user.id,
                'user_name': user.user_name,
                'message': f'User {user.user_name} created successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def update_user(user_id: str, user_config: dict) -> dict:
        """Update an existing user.

        Args:
            user_id: The ID of the user to update
            user_config: Dictionary containing updated user configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.users.patch(user_id, **user_config)
            
            return {
                'status': 'success',
                'user_id': user_id,
                'message': f'User {user_id} updated successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_user(user_id: str) -> dict:
        """Delete a user from the workspace.

        Args:
            user_id: The ID of the user to delete

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.users.delete(user_id)
            
            return {
                'status': 'success',
                'user_id': user_id,
                'message': f'User {user_id} deleted successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Groups API Tools

    @mcp_server.tool
    def list_groups() -> dict:
        """List all groups in the workspace.

        Returns:
            Dictionary containing list of groups or error message
        """
        try:
            client = get_workspace_client()
            groups = list(client.groups.list())
            
            group_list = []
            for group in groups:
                group_info = {
                    'id': group.id,
                    'display_name': group.display_name,
                    'members': [member.dict() for member in group.members] if group.members else None,
                    'groups': [subgroup.dict() for subgroup in group.groups] if group.groups else None
                }
                group_list.append(group_info)

            return {
                'status': 'success',
                'groups': group_list,
                'count': len(group_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_group(group_id: str) -> dict:
        """Get details of a specific group.

        Args:
            group_id: The ID of the group to retrieve

        Returns:
            Dictionary with group details or error message
        """
        try:
            client = get_workspace_client()
            group = client.groups.get(group_id)
            
            return {
                'status': 'success',
                'group': {
                    'id': group.id,
                    'display_name': group.display_name,
                    'members': [member.dict() for member in group.members] if group.members else None,
                    'groups': [subgroup.dict() for subgroup in group.groups] if group.groups else None,
                    'roles': [role.dict() for role in group.roles] if group.roles else None,
                    'schemas': group.schemas
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def create_group(group_config: dict) -> dict:
        """Create a new group in the workspace.

        Args:
            group_config: Dictionary containing group configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            group = client.groups.create(**group_config)
            
            return {
                'status': 'success',
                'group_id': group.id,
                'display_name': group.display_name,
                'message': f'Group {group.display_name} created successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def update_group(group_id: str, group_config: dict) -> dict:
        """Update an existing group.

        Args:
            group_id: The ID of the group to update
            group_config: Dictionary containing updated group configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.groups.patch(group_id, **group_config)
            
            return {
                'status': 'success',
                'group_id': group_id,
                'message': f'Group {group_id} updated successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_group(group_id: str) -> dict:
        """Delete a group from the workspace.

        Args:
            group_id: The ID of the group to delete

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.groups.delete(group_id)
            
            return {
                'status': 'success',
                'group_id': group_id,
                'message': f'Group {group_id} deleted successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Service Principals API Tools

    @mcp_server.tool
    def list_service_principals() -> dict:
        """List all service principals in the workspace.

        Returns:
            Dictionary containing list of service principals or error message
        """
        try:
            client = get_workspace_client()
            service_principals = list(client.service_principals.list())
            
            sp_list = []
            for sp in service_principals:
                sp_info = {
                    'id': sp.id,
                    'application_id': sp.application_id,
                    'display_name': sp.display_name,
                    'active': sp.active,
                    'groups': [group.dict() for group in sp.groups] if sp.groups else None
                }
                sp_list.append(sp_info)

            return {
                'status': 'success',
                'service_principals': sp_list,
                'count': len(sp_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_service_principal(sp_id: str) -> dict:
        """Get details of a specific service principal.

        Args:
            sp_id: The ID of the service principal to retrieve

        Returns:
            Dictionary with service principal details or error message
        """
        try:
            client = get_workspace_client()
            sp = client.service_principals.get(sp_id)
            
            return {
                'status': 'success',
                'service_principal': {
                    'id': sp.id,
                    'application_id': sp.application_id,
                    'display_name': sp.display_name,
                    'active': sp.active,
                    'groups': [group.dict() for group in sp.groups] if sp.groups else None,
                    'roles': [role.dict() for role in sp.roles] if sp.roles else None,
                    'schemas': sp.schemas
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def create_service_principal(sp_config: dict) -> dict:
        """Create a new service principal in the workspace.

        Args:
            sp_config: Dictionary containing service principal configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            sp = client.service_principals.create(**sp_config)
            
            return {
                'status': 'success',
                'service_principal_id': sp.id,
                'application_id': sp.application_id,
                'display_name': sp.display_name,
                'message': f'Service principal {sp.display_name} created successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def update_service_principal(sp_id: str, sp_config: dict) -> dict:
        """Update an existing service principal.

        Args:
            sp_id: The ID of the service principal to update
            sp_config: Dictionary containing updated service principal configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.service_principals.patch(sp_id, **sp_config)
            
            return {
                'status': 'success',
                'service_principal_id': sp_id,
                'message': f'Service principal {sp_id} updated successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_service_principal(sp_id: str) -> dict:
        """Delete a service principal from the workspace.

        Args:
            sp_id: The ID of the service principal to delete

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.service_principals.delete(sp_id)
            
            return {
                'status': 'success',
                'service_principal_id': sp_id,
                'message': f'Service principal {sp_id} deleted successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Personal Access Tokens API Tools

    @mcp_server.tool
    def list_tokens() -> dict:
        """List all personal access tokens for the current user.

        Returns:
            Dictionary containing list of tokens or error message
        """
        try:
            client = get_workspace_client()
            tokens = list(client.tokens.list())
            
            token_list = []
            for token in tokens:
                token_info = {
                    'token_id': token.token_id,
                    'creation_time': token.creation_time,
                    'expiry_time': token.expiry_time,
                    'comment': token.comment
                }
                token_list.append(token_info)

            return {
                'status': 'success',
                'tokens': token_list,
                'count': len(token_list)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def create_token(comment: str = None, lifetime_seconds: int = None) -> dict:
        """Create a new personal access token.

        Args:
            comment: Optional comment for the token
            lifetime_seconds: Token lifetime in seconds (optional)

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            token_config = {}
            if comment:
                token_config['comment'] = comment
            if lifetime_seconds:
                token_config['lifetime_seconds'] = lifetime_seconds
                
            token = client.tokens.create(**token_config)
            
            return {
                'status': 'success',
                'token_id': token.token_id,
                'token_value': token.token_value,
                'comment': comment,
                'lifetime_seconds': lifetime_seconds,
                'message': f'Personal access token created successfully',
                'warning': 'Token value is only shown once. Please save it securely.'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def revoke_token(token_id: str) -> dict:
        """Revoke a personal access token.

        Args:
            token_id: The ID of the token to revoke

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.tokens.delete(token_id)
            
            return {
                'status': 'success',
                'token_id': token_id,
                'message': f'Personal access token {token_id} revoked successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # IP Access Lists API Tools

    @mcp_server.tool
    def list_ip_access_lists() -> dict:
        """List all IP access lists in the workspace.

        Returns:
            Dictionary containing list of IP access lists or error message
        """
        try:
            client = get_workspace_client()
            ip_lists = list(client.ip_access_lists.list())
            
            ip_list_info = []
            for ip_list in ip_lists:
                ip_info = {
                    'list_id': ip_list.list_id,
                    'label': ip_list.label,
                    'list_type': ip_list.list_type.name if ip_list.list_type else None,
                    'enabled': ip_list.enabled,
                    'ip_addresses': ip_list.ip_addresses,
                    'created_at': ip_list.created_at,
                    'created_by': ip_list.created_by,
                    'updated_at': ip_list.updated_at,
                    'updated_by': ip_list.updated_by
                }
                ip_list_info.append(ip_info)

            return {
                'status': 'success',
                'ip_access_lists': ip_list_info,
                'count': len(ip_list_info)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def get_ip_access_list(list_id: str) -> dict:
        """Get details of a specific IP access list.

        Args:
            list_id: The ID of the IP access list to retrieve

        Returns:
            Dictionary with IP access list details or error message
        """
        try:
            client = get_workspace_client()
            ip_list = client.ip_access_lists.get(list_id)
            
            return {
                'status': 'success',
                'ip_access_list': {
                    'list_id': ip_list.list_id,
                    'label': ip_list.label,
                    'list_type': ip_list.list_type.name if ip_list.list_type else None,
                    'enabled': ip_list.enabled,
                    'ip_addresses': ip_list.ip_addresses,
                    'created_at': ip_list.created_at,
                    'created_by': ip_list.created_by,
                    'updated_at': ip_list.updated_at,
                    'updated_by': ip_list.updated_by
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def create_ip_access_list(ip_config: dict) -> dict:
        """Create a new IP access list.

        Args:
            ip_config: Dictionary containing IP access list configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            ip_list = client.ip_access_lists.create(**ip_config)
            
            return {
                'status': 'success',
                'list_id': ip_list.list_id,
                'label': ip_list.label,
                'message': f'IP access list {ip_list.label} created successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def update_ip_access_list(list_id: str, ip_config: dict) -> dict:
        """Update an existing IP access list.

        Args:
            list_id: The ID of the IP access list to update
            ip_config: Dictionary containing updated IP access list configuration

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            
            # Add list_id to config
            ip_config['list_id'] = list_id
            
            client.ip_access_lists.replace(**ip_config)
            
            return {
                'status': 'success',
                'list_id': list_id,
                'message': f'IP access list {list_id} updated successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def delete_ip_access_list(list_id: str) -> dict:
        """Delete an IP access list.

        Args:
            list_id: The ID of the IP access list to delete

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.ip_access_lists.delete(list_id)
            
            return {
                'status': 'success',
                'list_id': list_id,
                'message': f'IP access list {list_id} deleted successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # Workspace Configuration API Tools

    @mcp_server.tool
    def get_workspace_conf() -> dict:
        """Get workspace configuration settings.

        Returns:
            Dictionary with workspace configuration or error message
        """
        try:
            client = get_workspace_client()
            conf = client.workspace_conf.get_status()
            
            return {
                'status': 'success',
                'workspace_configuration': dict(conf) if conf else {}
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @mcp_server.tool
    def set_workspace_conf(config_key: str, config_value: str) -> dict:
        """Set a workspace configuration setting.

        Args:
            config_key: Configuration key to set
            config_value: Configuration value to set

        Returns:
            Dictionary with operation result or error message
        """
        try:
            client = get_workspace_client()
            client.workspace_conf.set_status({config_key: config_value})
            
            return {
                'status': 'success',
                'config_key': config_key,
                'config_value': config_value,
                'message': f'Workspace configuration {config_key} set successfully'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}