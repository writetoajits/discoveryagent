import google.auth
import re
import sys
import os
from google.adk.agents import Agent
from google.adk.tools.agent_tool import AgentTool
from google.adk.tools.bigquery import BigQueryToolset, BigQueryCredentialsConfig
from google.cloud import dataplex_v1
from google.cloud import datacatalog_lineage_v1
from google.api_core import exceptions

# Ensure Python treats the root workspace folder as a discoverable package path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# 1. Setup Live BigQuery Tools
credentials, project_id = google.auth.default()
credentials_config = BigQueryCredentialsConfig(credentials=credentials)
bq_toolset = BigQueryToolset(credentials_config=credentials_config)

# 2. Import internal Discovery Agent (Absolute import prevents path errors)
from knowledge_catalog_discovery_agent.agent import discovery_agent

# 3. Import newly built Automation Agent
from metadata_automation_agent.agent import root_agent as automation_sub_agent


# 4. Programmatic Tool to Retrieve LLM-Ready Metadata Context
def get_table_dataplex_context(table_path: str) -> str:
    """
    Retrieves rich YAML metadata context (including active Data Quality Scorecards)
    from the Dataplex Knowledge Catalog for a specific BigQuery table entry.
    """
    try:
        client = dataplex_v1.CatalogServiceClient(credentials=credentials)
        clean_path = re.sub(r'[*`\s]', '', table_path)
        parts = clean_path.split('.')
        if len(parts) != 3:
            return f"Invalid path format '{table_path}'. Please use 'project.dataset.table'."
            
        p_id, d_id, t_id = parts[0], parts[1], parts[2]
        parent_location = f"projects/{p_id}/locations/europe-west2"
        
        resource_uri = f"{parent_location}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{p_id}/datasets/{d_id}/tables/{t_id}"
        
        request = dataplex_v1.LookupContextRequest(
            name=parent_location,
            resources=[resource_uri],
            options={"format": "yaml", "budget": "4000"}
        )
        
        response = client.lookup_context(request=request)
        return response.context if response.context else "No active metadata context returned for this asset."
    except Exception as e:
        return f"Failed to retrieve context via Dataplex backend API: {str(e)}"


# =====================================================================
# TOOL 2: Query Live Upstream & Downstream Data Lineage Mappings
# =====================================================================
def get_table_data_lineage(table_path: str) -> dict:
    """
    Queries the GCP Data Lineage API to extract upstream sources and downstream 
    consumers for a target BigQuery table, detailing the pipeline processes involved.
    """
    try:
        client = datacatalog_lineage_v1.LineageClient(credentials=credentials)
        clean_path = re.sub(r'[*`\s]', '', table_path)
        parts = clean_path.split('.')
        if len(parts) != 3:
            return {"error": f"Invalid format '{table_path}'. Use 'project.dataset.table'."}
            
        p_id, d_id, t_id = parts[0], parts[1], parts[2]
        target_fqn = f"bigquery:{p_id}.{d_id}.{t_id}"
        parent = f"projects/{p_id}/locations/us"
        
        lineage_summary = {
            "target_asset": target_fqn,
            "upstream_sources": [],
            "downstream_consumers": []
        }
        
        # 1. Fetch Upstream Lineage links
        upstream_request = datacatalog_lineage_v1.SearchLinksRequest(
            parent=parent,
            target=datacatalog_lineage_v1.EntityReference(fully_qualified_name=target_fqn)
        )
        try:
            links = client.search_links(request=upstream_request)
            for link in links:
                lineage_summary["upstream_sources"].append({
                    "source": link.source.fully_qualified_name,
                    "process": link.name.split('/')[-1] if '/' in link.name else link.name
                })
        except exceptions.GoogleAPICallError as e:
            lineage_summary["upstream_error"] = str(e)

        # 2. Fetch Downstream Lineage links
        downstream_request = datacatalog_lineage_v1.SearchLinksRequest(
            parent=parent,
            source=datacatalog_lineage_v1.EntityReference(fully_qualified_name=target_fqn)
        )
        try:
            links = client.search_links(request=downstream_request)
            for link in links:
                lineage_summary["downstream_consumers"].append({
                    "target": link.target.fully_qualified_name,
                    "process": link.name.split('/')[-1] if '/' in link.name else link.name
                })
        except exceptions.GoogleAPICallError as e:
            lineage_summary["downstream_error"] = str(e)
            
        return lineage_summary
    except Exception as e:
        return {"error": f"Failed to execute Lineage lookup mapping: {str(e)}"}



# =====================================================================
# TOOL 3: Discover Transformation SQL parameters via Link Contexts
# =====================================================================
def get_lineage_process_transformation(table_path: str) -> str:
    """
    Leverages BatchSearchLinkProcesses via link strings to extract the exact 
    underlying transformation code logic responsible for generating a target asset.
    
    Args:
        table_path: Full dot-notation resource path (e.g., 'project.dataset.table')
    """
    try:
        from google.cloud import bigquery
        
        client = datacatalog_lineage_v1.LineageClient(credentials=credentials)
        bq_client = bigquery.Client(credentials=credentials, project=project_id)
        
        clean_path = re.sub(r'[*`\s]', '', table_path)
        parts = clean_path.split('.')
        if len(parts) != 3:
            return f"Invalid format '{table_path}'. Use 'project.dataset.table'."
            
        p_id, d_id, t_id = parts[0], parts[1], parts[2]
        target_fqn = f"bigquery:{p_id}.{d_id}.{t_id}"
        parent = f"projects/{p_id}/locations/us"
        
        # 1. Search for the raw link strings mapped to this asset
        search_request = datacatalog_lineage_v1.SearchLinksRequest(
            parent=parent,
            target=datacatalog_lineage_v1.EntityReference(fully_qualified_name=target_fqn)
        )
        links_pager = client.search_links(request=search_request)
        link_names = [link.name if hasattr(link, 'name') else str(link) for link in links_pager]
        
        if not link_names:
            return f"No active upstream data lineage links found for asset: {target_fqn}"
            
        # 2. Package strings straight into the Batch Search Request
        batch_request = datacatalog_lineage_v1.BatchSearchLinkProcessesRequest(
            parent=parent,
            links=link_names
        )
        process_links_pages = client.batch_search_link_processes(request=batch_request)
        
        summary = f"### 🔄 Transformation Mapping for Target Table: `{t_id}`\n\n"
        found_process = False
        
        for process_link in process_links_pages:
            found_process = True
            full_process_path = process_link.process
            
            # Inflate the process object to grab attributes
            proc_request = datacatalog_lineage_v1.GetProcessRequest(name=full_process_path)
            proc_meta = client.get_process(request=proc_request)
            p_token = proc_meta.name.split('/')[-1]
            
            summary += f"#### ⚙️ Operation Name: {proc_meta.display_name if proc_meta.display_name else p_token}\n"
            
            attributes = dict(proc_meta.attributes)
            sql_query = attributes.get("sql") or attributes.get("query")
            bq_job = attributes.get("bigquery_job_id")
            
            # Case A: If a standard single-statement query ran, display it directly
            if sql_query:
                summary += f"  - **TRANSFORMATION SQL:**\n  ```sql\n  {sql_query}\n  ```\n"
            
            # Case B: Multi-statement Script Job ID context found (Runs BQ Information Schema Check)
            elif bq_job:
                try:
                    info_schema_query = f"""
                    SELECT query 
                    FROM `{p_id}.region-{LOCATION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
                    WHERE job_id = '{bq_job}' or parent_job_id = '{bq_job}'
                    LIMIT 1
                    """
                    query_job = bq_client.query(info_schema_query)
                    results = list(query_job.result())
                    
                    if results and results[0].query:
                        summary += f"  - **COMPILED SCRIPT TRANSFORMATION SQL:**\n  ```sql\n  {results[0].query}\n  ```\n"
                    else:
                        summary += f"  - **BigQuery Job ID Reference:** `{bq_job}` (Log metrics details are outside active history window)\n"
                except Exception:
                    summary += f"  - **BigQuery Job ID Reference:** `{bq_job}`\n"
            
            # Case C: DDL/DML Dynamic View Definitions Fallback
            else:
                try:
                    table_ref = bq_client.get_table(f"{p_id}.{d_id}.{t_id}")
                    if table_ref.table_type == "VIEW" and table_ref.view_query:
                        summary += f"  - **VIEW DEFINITION SQL:**\n  ```sql\n  {table_ref.view_query}\n  ```\n"
                    else:
                        summary += "  - No raw transformation attributes logged on this node edge.\n"
                except Exception:
                    summary += "  - No raw transformation attributes logged on this node edge.\n"
                    
        return summary if found_process else f"No active transformation records found attached to the links of {t_id}."
        
    except Exception as e:
        return f"Failed to execute transformation mapping lookup via client: {str(e)}"


# =====================================================================
# Optimized Master Orchestrator Root Agent
# =====================================================================
root_agent = Agent(
    name="gcp_petabyte_migration_agent",
    model="gemini-2.5-flash",
    instruction="""
    You are a Data Discovery Agent. You minimize latency by executing ONLY the single most relevant tool required. Never daisy-chain or execute multiple data tools sequentially unless explicitly forced.
    
    CRITICAL ROUTING RULES:
    1. CONCEPTS / BROAD SEARCH: If a user asks to 'search' or find assets by keyword conceptually, route ONLY to `knowledge_catalog_discovery_agent`. Stop immediately after receiving its output.
    2. SUMMARY / AUDIT / PROFILE: If a user provides an explicit table path and wants an executive summary, schemas, or profile ratios, run ONLY `get_table_dataplex_context`. Do not run any other technical lookup tools.
    3. DATA LINEAGE / TRANSFORMATION: If a user explicitly requests a pipeline flow map or transformation code queries, run ONLY `get_lineage_process_transformation`.
    
    EVALUATION CONSTRAINT:
    Never guess parameters. If a table path is missing from a multi-part user request, run the search spoken sub-agent first, yield the found path to the user, and wait for them to confirm before attempting line lookups.
    """,
    tools=[
        AgentTool(agent=discovery_agent),
        AgentTool(agent=automation_sub_agent), 
        get_table_dataplex_context,            
        get_table_data_lineage,
        get_lineage_process_transformation,    
        bq_toolset                             
    ]
)