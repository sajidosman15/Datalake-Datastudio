import os
import time 
import json
import requests

from structlog import get_logger
from dotenv import load_dotenv

# Load .env file
load_dotenv()
logger = get_logger()

def get_nifi_token():
    """
        This function generates an access token for communicating with NiFi.
    """
    TOKEN_URL = f"{os.getenv('NIFI_BASE_URL')}/access/token"
    data = {
        "username": os.getenv("NIFI_USERNAME"),
        "password": os.getenv("NIFI_PASSWORD")
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        response = requests.post(TOKEN_URL, data=data, headers=headers, verify=False)
        if response.status_code == 201:
            logger.info(f"Successfully generated the access token.")
            return response.text
        else:
            logger.error(f"Failed to get token: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error connecting to NiFi: {e}")
        return None

def get_request_headers(token):
    """
        Initialize the API headers.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    return headers

def get_sql_template_payload():
    """
        This function creates a copy of the SQL Server template at position (0,0).
    """
    template_payload = {
        "originX": 0.0,
        "originY": 0.0,
        "templateId": os.getenv('SQL_TEMPLATE_ID')
    }
    return template_payload

def create_template_instance(headers):
    """
        Create the template in the NiFi workspace.
    """
    # For different connector select different template_payload
    template_payload = get_sql_template_payload()
    response = requests.post(f"{os.getenv('NIFI_BASE_URL')}/process-groups/{os.getenv('PROCESS_GROUP_ID')}/template-instance", 
                             json=template_payload, headers=headers, verify=False)

    if response.status_code == 201:
        flow_details = response.json()
        new_process_group_id = flow_details["flow"]["processGroups"][0]["id"]
        logger.info(f"Flow instantiated with ID: {new_process_group_id}")
        return new_process_group_id
    else:
        logger.error(f"Failed to instantiate flow: {response.text}")
        return None

def get_payload_from_variable_registry(headers, new_process_group_id, db_url, db_name, db_username, db_password, tables):
    """
        Set predefined variables in the template for later use.
    """
    var_response = requests.get(f"{os.getenv('NIFI_BASE_URL')}/process-groups/{new_process_group_id}/variable-registry", 
                                    headers=headers, verify=False)

    if var_response.status_code == 200:
        var_registry = var_response.json()
        var_revision = var_registry["processGroupRevision"]

        # convert the tables list to json.
        tables = json.dumps(tables)

        # Update Variables
        update_payload = {
            "processGroupRevision": var_revision,
            "variableRegistry": {
                "variables": [
                    {"variable": {"name": "DB", "value": db_url}},
                    {"variable": {"name": "DB_NAME", "value": db_name}},
                    {"variable": {"name": "USER", "value": db_username}},
                    {"variable": {"name": "PASS", "value": db_password}},
                    {"variable": {"name": "TABLES", "value": tables}}
                ],
                "processGroupId" : var_registry["variableRegistry"]['processGroupId']
            }
        }

        logger.info(f"Successfully feached template variable registry")
        return update_payload
    else:
        logger.error(f"Failed to get template variable registry: {var_response.text}")
        return None
    
def update_template_variable_registry(headers, new_process_group_id, update_payload):
    """
        Update the variables in the template's variable registry.
    """
    update_response = requests.put(f"{os.getenv('NIFI_BASE_URL')}/process-groups/{new_process_group_id}/variable-registry", 
                                            json=update_payload, headers=headers, verify=False)

    if update_response.status_code == 200:
        logger.info("Flow variables updated successfully")
        return True
    else:
        logger.error(f"Failed to update flow variables: {update_response.text}")
        return False
    
def get_controller_services_of_template(headers, process_group_id):
    """
        Fetch all controller services from the template.
    """
    response = requests.get(f"{os.getenv('NIFI_BASE_URL')}/flow/process-groups/{process_group_id}/controller-services", 
                            headers=headers, verify=False)
    
    if response.status_code == 200:
        services = response.json()["controllerServices"]
        filtered_services = [svc for svc in services if svc.get("parentGroupId") == process_group_id]
        logger.info("Successfully feached all controller services.")
        return filtered_services
    else:
        logger.error(f"Failed to fetch controller services: {response.text}")
        return None

def enable_controller_services_of_template(headers, process_group_id):
    """
        Enable all the services within the template.
    """
    services = get_controller_services_of_template(headers, process_group_id)

    if services:
        all_service_is_enabled = True
        for service in services:
            service_id = service["id"]
            service_state = service["component"]["state"]
            service_type = service["component"].get("type")
            properties = service["component"].get("properties", {})

            if service_type == "org.apache.nifi.dbcp.DBCPConnectionPool":
                properties["Password"] = "${PASS}"

            # Enable only if it's DISABLED
            if service_state == "DISABLED":
                enable_payload = {
                    "revision": {"version": service["revision"]["version"]},
                    "component": {
                        "id": service_id,
                        "properties": properties,
                        "state": "ENABLED"
                    }
                }

                enable_response = requests.put(f"{os.getenv('NIFI_BASE_URL')}/controller-services/{service_id}", 
                                               json=enable_payload, headers=headers, verify=False)

                if enable_response.status_code == 200:
                    logger.info(f"Enabled Controller Service: {service['component']['name']}")
                else:
                    all_service_is_enabled = False
                    logger.error(f"Failed to enable service {service['component']['name']}: {enable_response.text}")

        # Wait for services to fully enable before proceeding
        time.sleep(5)
        return all_service_is_enabled
    else:
        return False
    
def start_the_process(headers, new_process_group_id):
    """
        Run all processors within the template.
    """
    start_payload = {"id": new_process_group_id, "state": "RUNNING"}
    start_response = requests.put(f"{os.getenv('NIFI_BASE_URL')}/flow/process-groups/{new_process_group_id}", 
                                json=start_payload, headers=headers, verify=False)
    
    if start_response.status_code == 200:
        logger.info("Flow started successfully!")
        return True
    else:
        logger.error(f"Failed to start flow: {start_response.text}")
        return False


def instantiate_flow(db_url,db_name,db_username,db_password,tables):
    token = get_nifi_token()
    headers = get_request_headers(token)
    new_process_group_id = create_template_instance(headers)
    if new_process_group_id:
        update_payload = get_payload_from_variable_registry(headers, new_process_group_id, db_url, db_name, db_username, db_password, tables)
        if update_payload:
            variable_updated = update_template_variable_registry(headers, new_process_group_id, update_payload)
            if variable_updated:
                all_service_is_enabled = enable_controller_services_of_template(headers, new_process_group_id)
                if all_service_is_enabled:
                    process_started = start_the_process(headers, new_process_group_id)
                    #Task: If process started store it to database
                else:
                    logger.error(f"Failed to start flow because all controller is not enabled.")
                    

