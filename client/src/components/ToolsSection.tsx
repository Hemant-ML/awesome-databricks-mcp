import { useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Search, Database, Shield, Briefcase, GitBranch, Brain, HardDrive, BarChart, Settings, FileText, Activity, Wrench } from "lucide-react";

interface MCPTool {
  name: string;
  description: string;
}

interface ToolsSectionProps {
  tools: MCPTool[];
  servername: string;
}

// Tool categories with their metadata
const TOOL_CATEGORIES = {
  security: {
    icon: Shield,
    color: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300",
    count: 35,
    description: "User management, secrets, tokens, permissions"
  },
  compute: {
    icon: Settings,
    color: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300", 
    count: 25,
    description: "Clusters, instance pools, libraries, policies"
  },
  workspace: {
    icon: Briefcase,
    color: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300",
    count: 21,
    description: "Files, repositories, permissions, settings"
  },
  mlflow: {
    icon: Brain,
    color: "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300",
    count: 21,
    description: "Model registry, experiments, runs, versions"
  },
  unity_catalog: {
    icon: Database,
    color: "bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-300",
    count: 21,
    description: "Catalogs, schemas, tables, volumes, functions"
  },
  jobs_pipelines: {
    icon: GitBranch,
    color: "bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300",
    count: 19,
    description: "Jobs, DLT pipelines, runs, scheduling"
  },
  data_management: {
    icon: HardDrive,
    color: "bg-teal-100 text-teal-800 dark:bg-teal-900 dark:text-teal-300",
    count: 15,
    description: "DBFS, storage, external locations"
  },
  sql_operations: {
    icon: FileText,
    color: "bg-cyan-100 text-cyan-800 dark:bg-cyan-900 dark:text-cyan-300",
    count: 15,
    description: "SQL warehouses, queries, monitoring"
  },
  governance: {
    icon: Activity,
    color: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300",
    count: 11,
    description: "Audit logs, lineage, system tables"
  },
  dashboards: {
    icon: BarChart,
    color: "bg-pink-100 text-pink-800 dark:bg-pink-900 dark:text-pink-300",
    count: 11,
    description: "Lakeview and legacy dashboards"
  },
  core: {
    icon: Wrench,
    color: "bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300",
    count: 1,
    description: "Health checks"
  }
};

// Exact tool mappings based on actual module definitions
const TOOL_MAPPINGS: { [key: string]: string } = {
  // Core tools (1)
  'health': 'core',
  
  // Security tools (35)
  'list_secret_scopes': 'security',
  'create_secret_scope': 'security',
  'delete_secret_scope': 'security',
  'list_secrets': 'security',
  'put_secret': 'security',
  'delete_secret': 'security',
  'get_secret_acl': 'security',
  'list_secret_acls': 'security',
  'put_secret_acl': 'security',
  'delete_secret_acl': 'security',
  'list_users': 'security',
  'get_user': 'security',
  'create_user': 'security',
  'update_user': 'security',
  'delete_user': 'security',
  'list_groups': 'security',
  'get_group': 'security',
  'create_group': 'security',
  'update_group': 'security',
  'delete_group': 'security',
  'list_service_principals': 'security',
  'get_service_principal': 'security',
  'create_service_principal': 'security',
  'update_service_principal': 'security',
  'delete_service_principal': 'security',
  'list_tokens': 'security',
  'create_token': 'security',
  'revoke_token': 'security',
  'list_ip_access_lists': 'security',
  'get_ip_access_list': 'security',
  'create_ip_access_list': 'security',
  'update_ip_access_list': 'security',
  'delete_ip_access_list': 'security',
  'get_workspace_conf': 'security',
  'set_workspace_conf': 'security',
  
  // Compute tools (25)
  'list_clusters': 'compute',
  'get_cluster': 'compute',
  'create_cluster': 'compute',
  'start_cluster': 'compute',
  'restart_cluster': 'compute',
  'terminate_cluster': 'compute',
  'delete_cluster': 'compute',
  'edit_cluster': 'compute',
  'get_cluster_events': 'compute',
  'list_instance_pools': 'compute',
  'get_instance_pool': 'compute',
  'create_instance_pool': 'compute',
  'edit_instance_pool': 'compute',
  'delete_instance_pool': 'compute',
  'list_cluster_libraries': 'compute',
  'install_cluster_libraries': 'compute',
  'uninstall_cluster_libraries': 'compute',
  'list_all_cluster_libraries': 'compute',
  'list_cluster_policies': 'compute',
  'get_cluster_policy': 'compute',
  'create_cluster_policy': 'compute',
  'edit_cluster_policy': 'compute',
  'delete_cluster_policy': 'compute',
  'list_node_types': 'compute',
  'list_spark_versions': 'compute',
  
  // Workspace tools (21)
  'list_workspace_objects': 'workspace',
  'get_workspace_object': 'workspace',
  'export_workspace_object': 'workspace',
  'import_workspace_object': 'workspace',
  'delete_workspace_object': 'workspace',
  'create_workspace_directory': 'workspace',
  'list_repos': 'workspace',
  'get_repo': 'workspace',
  'create_repo': 'workspace',
  'update_repo': 'workspace',
  'delete_repo': 'workspace',
  'get_repo_permissions': 'workspace',
  'set_repo_permissions': 'workspace',
  'update_repo_permissions': 'workspace',
  'get_workspace_permissions': 'workspace',
  'set_workspace_permissions': 'workspace',
  'update_workspace_permissions': 'workspace',
  'get_current_user': 'workspace',
  'get_workspace_settings': 'workspace',
  'update_token_management_settings': 'workspace',
  'update_ip_access_list_settings': 'workspace',
  
  // MLflow tools (21)
  'list_models': 'mlflow',
  'get_model': 'mlflow',
  'create_model': 'mlflow',
  'update_model': 'mlflow',
  'delete_model': 'mlflow',
  'list_model_versions': 'mlflow',
  'get_model_version': 'mlflow',
  'create_model_version': 'mlflow',
  'update_model_version': 'mlflow',
  'delete_model_version': 'mlflow',
  'transition_model_version_stage': 'mlflow',
  'list_experiments': 'mlflow',
  'get_experiment': 'mlflow',
  'create_experiment': 'mlflow',
  'update_experiment': 'mlflow',
  'delete_experiment': 'mlflow',
  'restore_experiment': 'mlflow',
  'search_runs': 'mlflow',
  'get_run': 'mlflow',
  'delete_run': 'mlflow',
  'restore_run': 'mlflow',
  
  // Unity Catalog tools (21)
  'list_uc_catalogs': 'unity_catalog',
  'describe_uc_catalog': 'unity_catalog',
  'list_uc_schemas': 'unity_catalog',
  'describe_uc_schema': 'unity_catalog',
  'list_uc_tables': 'unity_catalog',
  'describe_uc_table': 'unity_catalog',
  'list_uc_volumes': 'unity_catalog',
  'describe_uc_volume': 'unity_catalog',
  'list_uc_functions': 'unity_catalog',
  'describe_uc_function': 'unity_catalog',
  'list_uc_models': 'unity_catalog',
  'describe_uc_model': 'unity_catalog',
  'list_uc_tags': 'unity_catalog',
  'apply_uc_tags': 'unity_catalog',
  'search_uc_objects': 'unity_catalog',
  'get_table_statistics': 'unity_catalog',
  'list_metastores': 'unity_catalog',
  'describe_metastore': 'unity_catalog',
  'list_data_quality_monitors': 'unity_catalog',
  'get_data_quality_results': 'unity_catalog',
  'create_data_quality_monitor': 'unity_catalog',
  
  // Jobs & Pipelines tools (19)
  'list_jobs': 'jobs_pipelines',
  'get_job': 'jobs_pipelines',
  'create_job': 'jobs_pipelines',
  'update_job': 'jobs_pipelines',
  'delete_job': 'jobs_pipelines',
  'list_job_runs': 'jobs_pipelines',
  'get_job_run': 'jobs_pipelines',
  'submit_job_run': 'jobs_pipelines',
  'cancel_job_run': 'jobs_pipelines',
  'get_job_run_logs': 'jobs_pipelines',
  'list_pipelines': 'jobs_pipelines',
  'get_pipeline': 'jobs_pipelines',
  'create_pipeline': 'jobs_pipelines',
  'update_pipeline': 'jobs_pipelines',
  'delete_pipeline': 'jobs_pipelines',
  'list_pipeline_runs': 'jobs_pipelines',
  'get_pipeline_run': 'jobs_pipelines',
  'start_pipeline_update': 'jobs_pipelines',
  'stop_pipeline_update': 'jobs_pipelines',
  
  // Data Management tools (15)
  'list_dbfs_files': 'data_management',
  'get_dbfs_file_info': 'data_management',
  'read_dbfs_file': 'data_management',
  'write_dbfs_file': 'data_management',
  'delete_dbfs_path': 'data_management',
  'create_dbfs_directory': 'data_management',
  'move_dbfs_path': 'data_management',
  'copy_dbfs_file': 'data_management',
  'list_external_locations': 'data_management',
  'list_volumes': 'data_management',
  'create_volume': 'data_management',
  'describe_external_location': 'data_management',
  'list_storage_credentials': 'data_management',
  'describe_storage_credential': 'data_management',
  'list_uc_permissions': 'data_management',
  
  // SQL Operations tools (15)
  'execute_dbsql': 'sql_operations',
  'list_warehouses': 'sql_operations',
  'get_sql_warehouse': 'sql_operations',
  'create_sql_warehouse': 'sql_operations',
  'start_sql_warehouse': 'sql_operations',
  'stop_sql_warehouse': 'sql_operations',
  'delete_sql_warehouse': 'sql_operations',
  'list_queries': 'sql_operations',
  'get_query': 'sql_operations',
  'get_query_results': 'sql_operations',
  'cancel_query': 'sql_operations',
  'get_statement_status': 'sql_operations',
  'get_statement_results': 'sql_operations',
  'cancel_statement': 'sql_operations',
  'list_recent_queries': 'sql_operations',
  
  // Governance tools (11)
  'list_system_schemas': 'governance',
  'query_audit_logs': 'governance',
  'query_table_lineage': 'governance',
  'query_column_lineage': 'governance',
  'query_table_usage': 'governance',
  'query_workspace_objects': 'governance',
  'query_permissions_changes': 'governance',
  'query_compute_usage': 'governance',
  'query_storage_usage': 'governance',
  'list_quality_monitors': 'governance',
  'get_quality_monitor_status': 'governance',
  
  // Dashboard tools (11)
  'list_lakeview_dashboards': 'dashboards',
  'get_lakeview_dashboard': 'dashboards',
  'create_lakeview_dashboard': 'dashboards',
  'update_lakeview_dashboard': 'dashboards',
  'delete_lakeview_dashboard': 'dashboards',
  'share_lakeview_dashboard': 'dashboards',
  'get_dashboard_permissions': 'dashboards',
  'list_dashboards': 'dashboards',
  'get_dashboard': 'dashboards',
  'create_dashboard': 'dashboards',
  'delete_dashboard': 'dashboards'
};

// Categorize tools based on exact mappings
function categorizeTools(tools: MCPTool[]) {
  const categorized: { [key: string]: MCPTool[] } = {};
  
  // Initialize categories
  Object.keys(TOOL_CATEGORIES).forEach(category => {
    categorized[category] = [];
  });

  tools.forEach(tool => {
    const category = TOOL_MAPPINGS[tool.name] || 'core';
    categorized[category].push(tool);
  });

  return categorized;
}

function formatToolName(name: string): string {
  return name
    .replace(/_/g, ' ')
    .replace(/\b\w/g, l => l.toUpperCase())
    .replace(/Uc /g, 'UC ')
    .replace(/Dbfs/g, 'DBFS')
    .replace(/Mlflow/g, 'MLflow')
    .replace(/Dlt/g, 'DLT')
    .replace(/Sql/g, 'SQL')
    .replace(/Api/g, 'API')
    .replace(/Ip/g, 'IP')
    .replace(/Acl/g, 'ACL');
}

export function ToolsSection({ tools, servername }: ToolsSectionProps) {
  const [searchTerm, setSearchTerm] = useState('');
  const [activeTab, setActiveTab] = useState('overview');
  
  const categorizedTools = categorizeTools(tools);
  
  // Filter tools based on search term
  const filteredTools = (categoryTools: MCPTool[]) => {
    if (!searchTerm) return categoryTools;
    return categoryTools.filter(tool => 
      tool.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      tool.description.toLowerCase().includes(searchTerm.toLowerCase())
    );
  };

  // Get total filtered count
  const getTotalFilteredCount = () => {
    if (!searchTerm) return tools.length;
    return Object.values(categorizedTools)
      .flat()
      .filter(tool => 
        tool.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
        tool.description.toLowerCase().includes(searchTerm.toLowerCase())
      ).length;
  };

  return (
    <div className="mb-12">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-2xl font-semibold flex items-center gap-2">
            <Wrench className="h-6 w-6" />
            Databricks MCP Tools
          </h2>
          <p className="text-muted-foreground mt-1">
            {getTotalFilteredCount()} of {tools.length} tools available
          </p>
        </div>
        <div className="relative w-64">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search tools..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="pl-10"
          />
        </div>
      </div>

      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList className="grid w-full grid-cols-12">
          <TabsTrigger value="overview" className="text-xs">Overview</TabsTrigger>
          <TabsTrigger value="security" className="text-xs">Security</TabsTrigger>
          <TabsTrigger value="compute" className="text-xs">Compute</TabsTrigger>
          <TabsTrigger value="workspace" className="text-xs">Workspace</TabsTrigger>
          <TabsTrigger value="mlflow" className="text-xs">MLflow</TabsTrigger>
          <TabsTrigger value="unity_catalog" className="text-xs">UC</TabsTrigger>
          <TabsTrigger value="jobs_pipelines" className="text-xs">Jobs</TabsTrigger>
          <TabsTrigger value="data_management" className="text-xs">Data</TabsTrigger>
          <TabsTrigger value="sql_operations" className="text-xs">SQL</TabsTrigger>
          <TabsTrigger value="governance" className="text-xs">Gov</TabsTrigger>
          <TabsTrigger value="dashboards" className="text-xs">Dash</TabsTrigger>
          <TabsTrigger value="core" className="text-xs">Core</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="mt-6">
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
            {Object.entries(TOOL_CATEGORIES).map(([category, meta]) => {
              const Icon = meta.icon;
              const categoryTools = categorizedTools[category] || [];
              const filteredCount = filteredTools(categoryTools).length;
              
              if (searchTerm && filteredCount === 0) return null;
              
              return (
                <Card 
                  key={category}
                  className="cursor-pointer hover:shadow-lg transition-shadow"
                  onClick={() => setActiveTab(category)}
                >
                  <CardHeader className="pb-3">
                    <div className="flex items-center justify-between">
                      <Icon className="h-5 w-5 text-muted-foreground" />
                      <Badge className={meta.color}>
                        {filteredCount}/{categoryTools.length}
                      </Badge>
                    </div>
                    <CardTitle className="text-lg capitalize">
                      {category.replace('_', ' ')}
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <CardDescription className="text-sm">
                      {meta.description}
                    </CardDescription>
                  </CardContent>
                </Card>
              );
            })}
          </div>
        </TabsContent>

        {Object.entries(TOOL_CATEGORIES).map(([category, meta]) => {
          const categoryTools = categorizedTools[category] || [];
          const filtered = filteredTools(categoryTools);
          const Icon = meta.icon;
          
          return (
            <TabsContent key={category} value={category} className="mt-6">
              <div className="mb-4">
                <div className="flex items-center gap-2 mb-2">
                  <Icon className="h-5 w-5" />
                  <h3 className="text-lg font-semibold capitalize">
                    {category.replace('_', ' ')} Tools
                  </h3>
                  <Badge className={meta.color}>
                    {filtered.length} tools
                  </Badge>
                </div>
                <p className="text-muted-foreground text-sm">{meta.description}</p>
              </div>

              {filtered.length === 0 ? (
                <div className="text-center text-muted-foreground py-8">
                  {searchTerm ? 'No tools match your search.' : 'No tools found in this category.'}
                </div>
              ) : (
                <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
                  {filtered.map((tool) => (
                    <Card key={tool.name} className="hover:shadow-md transition-shadow">
                      <CardHeader className="pb-3">
                        <CardTitle className="text-base leading-tight">
                          {formatToolName(tool.name)}
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        <CardDescription className="text-xs leading-relaxed mb-2">
                          {tool.description}
                        </CardDescription>
                        <div className="mt-3 pt-2 border-t border-muted">
                          <code className="text-xs font-mono text-muted-foreground bg-muted px-2 py-1 rounded">
                            /{servername}:{tool.name}
                          </code>
                        </div>
                      </CardContent>
                    </Card>
                  ))}
                </div>
              )}
            </TabsContent>
          );
        })}
      </Tabs>
    </div>
  );
}