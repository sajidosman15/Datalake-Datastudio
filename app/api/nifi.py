import requests
import time 
import os

from structlog import get_logger
from dotenv import load_dotenv

# Load .env file
load_dotenv()
logger = get_logger()

# Function to get Bearer Token
def get_nifi_token():
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
            return response.text  # NiFi returns the token as plain text
        else:
            logger.error(f"Failed to get token: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error connecting to NiFi: {e}")
        return None
    
# Function to enable all controller services in the new process group
def enable_controller_services(token, process_group_id):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Get all controller services in the process group
    response = requests.get(f"{os.getenv('NIFI_BASE_URL')}/flow/process-groups/{process_group_id}/controller-services", 
                            headers=headers, verify=False)

    if response.status_code == 200:
        services = response.json()["controllerServices"]
        for service in services:
            service_id = service["id"]
            service_state = service["component"]["state"]
            properties = service["component"].get("properties", {})

            if "Password" in properties:
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
                    logger.error(f"Failed to enable service {service['component']['name']}: {enable_response.text}")

        # Wait for services to fully enable before proceeding
        time.sleep(5)  # Adjust time if necessary
    else:
        logger.error(f"Failed to fetch controller services: {response.text}")

def instantiate_flow(db_url,db_name,db_username,db_password):
    token = get_nifi_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Step 1: Instantiate Flow from Template
    template_payload = {
        "originX": 0.0,
        "originY": 0.0,
        "templateId": os.getenv('SQL_TEMPLATE_ID')
    }
    response = requests.post(f"{os.getenv('NIFI_BASE_URL')}/process-groups/{os.getenv('PROCESS_GROUP_ID')}/template-instance", 
                             json=template_payload, headers=headers, verify=False)

    if response.status_code == 201:
        flow_details = response.json()
        new_process_group_id = flow_details["flow"]["processGroups"][0]["id"]
        logger.info(f"Flow instantiated with ID: {new_process_group_id}")
    else:
        logger.error(f"Failed to instantiate flow: {response.text}")
        return

    # Step 2: Fetch Flow Variables
    var_response = requests.get(f"{os.getenv('NIFI_BASE_URL')}/process-groups/{new_process_group_id}/variable-registry", 
                                headers=headers, verify=False)

    if var_response.status_code == 200:
        var_registry = var_response.json()
        var_revision = var_registry["processGroupRevision"]

        # Update Variables
        update_payload = {
            "processGroupRevision": var_revision,
            "variableRegistry": {
                "variables": [
                    {"variable": {"name": "DB", "value": db_url}},
                    {"variable": {"name": "DB_NAME", "value": db_name}},
                    {"variable": {"name": "USER", "value": db_username}},
                    {"variable": {"name": "PASS", "value": db_password}}
                ],
                "processGroupId" : var_registry["variableRegistry"]['processGroupId']
            }
        }

        update_response = requests.put(f"{os.getenv('NIFI_BASE_URL')}/process-groups/{new_process_group_id}/variable-registry", 
                                       json=update_payload, headers=headers, verify=False)

        if update_response.status_code == 200:
            # Enable all controller services
            enable_controller_services(token, new_process_group_id)
            logger.info("Flow variables updated successfully")
        else:
            logger.error(f"Failed to update flow variables: {update_response.text}")

    # Step 3: Start the Process Group
    start_payload = {"id": new_process_group_id, "state": "RUNNING"}
    start_response = requests.put(f"{os.getenv('NIFI_BASE_URL')}/flow/process-groups/{new_process_group_id}", 
                                  json=start_payload, headers=headers, verify=False)

    if start_response.status_code == 200:
        logger.info("Flow started successfully!")
    else:
        logger.error(f"Failed to start flow: {start_response.text}")
