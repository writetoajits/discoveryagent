import google.auth
import re
from google.adk.agents import Agent
from google.cloud import dataplex_v1

credentials, project_id = google.auth.default()

def manage_dataplex_metadata_scan(target_resource: str, action_mode: str, scan_type: str) -> str:
    """
    Manages Dataplex metadata scans (PROFILE or INSIGHTS) for BigQuery assets.
    """
    try:
        client = dataplex_v1.DataplexServiceClient(credentials=credentials)
        clean_target = re.sub(r'[*`\s]', '', target_resource)
        parts = clean_target.split('.')
        if len(parts) < 2:
            return f"Invalid target asset format: {target_resource}"
            
        p_id, d_id = parts[0], parts[1]
        t_id = parts[2] if len(parts) == 3 else None
        
        scan_base = f"di-{d_id.lower().replace('_', '-')}"
        if t_id:
            scan_base += f"-{t_id.lower().replace('_', '-')}"
        scan_id = f"{scan_base}-{scan_type.lower()}"[:63]
        
        parent_path = f"projects/{p_id}/locations/europe-west2"
        scan_path = f"{parent_path}/dataScans/{scan_id}"
        
        if action_mode.upper() == "CHECK":
            try:
                scan = client.get_data_scan(name=scan_path)
                return f"SUCCESS: Found active {scan_type} scan configuration. Scan ID: {scan.name}"
            except Exception:
                return f"NOT_FOUND: No active {scan_type} scan config exists for {clean_target}."
                
        elif action_mode.upper() == "RUN":
            if t_id:
                source_uri = f"//bigquery.googleapis.com/projects/{p_id}/datasets/{d_id}/tables/{t_id}"
            else:
                source_uri = f"//bigquery.googleapis.com/projects/{p_id}/datasets/{d_id}"
                
            scan_obj = dataplex_v1.DataScan()
            scan_obj.data_resource_spec = {"resource": source_uri}
            scan_obj.description = f"Automated structural {scan_type} scan provisioned via AI agent."
            
            if scan_type.upper() == "PROFILE":
                scan_obj.data_profile_spec = {"post_scan_actions": {"catalog_publishing_enabled": True}}
            else:
                scan_obj.data_insights_spec = {"post_scan_actions": {"catalog_publishing_enabled": True}}
                
            try:
                client.get_data_scan(name=scan_path)
            except Exception:
                create_req = dataplex_v1.CreateDataScanRequest(
                    parent=parent_path,
                    data_scan=scan_obj,
                    data_scan_id=scan_id
                )
                operation = client.create_data_scan(request=create_req)
                operation.result(timeout=90)
                
            run_req = dataplex_v1.RunDataScanRequest(name=scan_path)
            client.run_data_scan(request=run_req)
            return f"SUCCESS: Dispatched active {scan_type} scan backend cluster job for {clean_target}."
    except Exception as e:
        return f"ERROR executing automation step: {str(e)}"

root_agent = Agent(
    name="dataplex_metadata_automation_agent",
    model="gemini-2.5-flash",
    instruction="""
    You are a secure data engineer sub-agent responsible for checking or triggering Dataplex scans.
    You execute target parameters via 'manage_dataplex_metadata_scan'.
    """,
    tools=[manage_dataplex_metadata_scan]
)