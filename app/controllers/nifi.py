import time 
import json
import requests
import threading

from structlog import get_logger

from app.config import config
from app.models.connection import Connection
from app.controllers.kafka import store_data_kafka_to_hadoop

logger = get_logger()

def get_nifi_token():
    """
        This function generates an access token for communicating with NiFi.
    """
    logger.info(f"Module:NiFiController. Generating NiFi Access Token.")
    TOKEN_URL = f"{config.nifi.get_api_url()}/access/token"
    data = {
        "username": config.nifi.username,
        "password": config.nifi.password
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        response = requests.post(TOKEN_URL, data=data, headers=headers, verify=False)
        if response.status_code == 201:
            logger.info(f"Module:NiFiController. Successfully generated the access token.")
            return response.text
        else:
            logger.error(f"Module:NiFiController. Failed to get token: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Module:NiFiController. Error connecting to NiFi: {e}")
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

def get_template_payload_for_source(source_type):
    """
        This function creates a copy of the specified source template at position (0,0).
    """
    if source_type == "Microsoft SQL Server":
        template_payload = {
            "originX": 0.0,
            "originY": 0.0,
            "templateId": config.nifi.sql_template_id
        }
    return template_payload

def create_template_instance(headers, source_type):
    """
        Create the template in the NiFi workspace.
    """
    logger.info(f"Module:NiFiController. Creating NiFi template instance.")
    try:
        # For different connector select different template_payload
        template_payload = get_template_payload_for_source(source_type)

        # Creating the template in root process group
        response = requests.post(f"{config.nifi.get_api_url()}/process-groups/{config.nifi.root}/template-instance", 
                                json=template_payload, headers=headers, verify=False)

        if response.status_code == 201:
            flow_details = response.json()
            process_group_id = flow_details["flow"]["processGroups"][0]["id"]
            logger.info(f"Module:NiFiController. Flow instantiated with ID: {process_group_id}")
            return process_group_id
        else:
            logger.error(f"Module:NiFiController. Failed to instantiate flow: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Module:NiFiController. Failed to instantiate flow: {e}")
        return False
    
def get_payload_from_variable_registry(headers, process_group_id):
    """
        Get the variables registry from the template.
    """
    logger.info(f"Module:NiFiController. Getting payload from NiFi variable registry.")
    try:
        var_response = requests.get(f"{config.nifi.get_api_url()}/process-groups/{process_group_id}/variable-registry", 
                                        headers=headers, verify=False)

        if var_response.status_code == 200:
            logger.info(f"Module:NiFiController. Successfully feached template variable registry")
            return var_response
        else:
            logger.error(f"Module:NiFiController. Failed to get template variable registry: {var_response.text}")
            return False
    except Exception as e:
        logger.error(f"Module:NiFiController. Failed to get template variable registry: {e}")
        return False

def set_payload_on_variable_registry(payload, connection:Connection):
    """
        Set predefined variables in the template for later use.
    """
    logger.info(f"Module:NiFiController. Setting variables on NiFi variable registry.")
    try:
        var_registry = payload.json()
        var_revision = var_registry["processGroupRevision"]
        connection_properties = connection.connection_properties

        if connection.source_type == "Microsoft SQL Server":
            # convert the tables list to json.
            tables = json.dumps(connection_properties['tables'])
            # Update Variables
            update_payload = {
                "processGroupRevision": var_revision,
                "variableRegistry": {
                    "variables": [
                        {"variable": {"name": "DB", "value": connection_properties['db_url']}},
                        {"variable": {"name": "DB_NAME", "value": connection_properties['db_name']}},
                        {"variable": {"name": "USER", "value": connection_properties['db_username']}},
                        {"variable": {"name": "PASS", "value": connection_properties['db_password']}},
                        {"variable": {"name": "TABLES", "value": tables}}
                    ],
                    "processGroupId" : var_registry["variableRegistry"]['processGroupId']
                }
            }
        logger.info(f"Module:NiFiController. Successfully set variables on NiFi variable registry.")
        return update_payload
    except Exception as e:
        logger.error(f"Module:NiFiController. Failed to set variable on NiFi variable registry: {e}")
        return False
    
def update_template_variable_registry(headers, process_group_id, update_payload):
    """
        Update the variables in the template's variable registry.
    """
    logger.info(f"Module:NiFiController. Updating NiFi template variable registry.")
    try:
        update_response = requests.put(f"{config.nifi.get_api_url()}/process-groups/{process_group_id}/variable-registry", 
                                                json=update_payload, headers=headers, verify=False)

        if update_response.status_code == 200:
            logger.info("Module:NiFiController. Flow variables updated successfully")
            return True
        else:
            logger.error(f"Module:NiFiController. Failed to update flow variables: {update_response.text}")
            return False
    except Exception as e:
        logger.error(f"Module:NiFiController. Failed to update flow variables: {e}")
        return False
    
def get_controller_services_of_template(headers, process_group_id):
    """
        Fetch all controller services from the template.
    """
    logger.info(f"Module:NiFiController. Getting controller services from NiFi template.")
    try:
        response = requests.get(f"{config.nifi.get_api_url()}/flow/process-groups/{process_group_id}/controller-services", 
                                headers=headers, verify=False)
        
        if response.status_code == 200:
            services = response.json()["controllerServices"]
            filtered_services = [svc for svc in services if svc.get("parentGroupId") == process_group_id]
            logger.info("Module:NiFiController. Successfully feached all controller services.")
            return filtered_services
        else:
            logger.error(f"Module:NiFiController. Failed to fetch controller services: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Module:NiFiController. Failed to fetch controller services: {e}")
        return False

def enable_controller_services_of_template(headers, process_group_id):
    """
        Enable all the services within the template.
    """
    logger.info(f"Module:NiFiController. Starting controller services of the NiFi template.")

    try:
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

                    enable_response = requests.put(f"{config.nifi.get_api_url()}/controller-services/{service_id}", 
                                                json=enable_payload, headers=headers, verify=False)

                    if enable_response.status_code == 200:
                        logger.info(f"Module:NiFiController. Enabled Controller Service: {service['component']['name']}")
                    else:
                        all_service_is_enabled = False
                        logger.error(f"Module:NiFiController. Failed to enable service {service['component']['name']}: {enable_response.text}")

            # Wait for services to fully enable before proceeding
            time.sleep(5)
            return all_service_is_enabled
        else:
            return False
    except Exception as e:
        logger.error(f"Module:NiFiController. Failed to enable all controller services: {e}")
        return False

def stop_all_processors(headers, process_group_id):
    """
        Stop all running processors in the specified process group.
    """
    logger.info(f"Module:NiFiController. Stopping all processors for process group {process_group_id}.")
    try:
        processors_resp = requests.get(
            f"{config.nifi.get_api_url()}/process-groups/{process_group_id}/processors",
            headers=headers, verify=False
        )

        if processors_resp.status_code != 200:
            logger.error(f"Module:NiFiController. Failed to retrieve processors: {processors_resp.text}")
            return False

        processors = processors_resp.json().get('processors', [])

        for proc in processors:
            proc_id = proc['component']['id']
            revision = proc['revision']
            current_state = proc['component'].get('state', '')

            if current_state == "RUNNING":
                stop_payload = {
                    "revision": revision,
                    "component": {
                        "id": proc_id,
                        "state": "STOPPED"
                    }
                }

                stop_resp = requests.put(
                    f"{config.nifi.get_api_url()}/processors/{proc_id}",
                    headers=headers, json=stop_payload, verify=False
                )

                if stop_resp.status_code != 200:
                    logger.error(f"Module:NiFiController. Failed to stop processor {proc_id}: {stop_resp.text}")
                    return False

        logger.info(f"Module:NiFiController. All processors stopped successfully.")
        return True

    except Exception as e:
        logger.error(f"Module:NiFiController. Exception while stopping processors: {e}")
        return False

def stop_all_services(headers, process_group_id):
    """
        Disable all controller services in the specified process group.
    """
    logger.info(f"Module:NiFiController. Disabling all controller services for process group {process_group_id}.")
    try:
        services = get_controller_services_of_template(headers, process_group_id)

        for service in services:
            service_id = service["id"]
            revision = service['revision']
            body = {
                "revision": revision,
                "component": {
                    "id": service_id,
                    "state": "DISABLED"
                }
            }

            update_response = requests.put(
                f"{config.nifi.get_api_url()}/controller-services/{service_id}",
                json=body, headers=headers, verify=False
            )

            if update_response.status_code != 200:
                logger.error(f"Module:NiFiController. Failed to disable service {service_id}: {update_response.text}")
                return False

        logger.info(f"Module:NiFiController. All controller services disabled successfully.")
        return True

    except Exception as e:
        logger.error(f"Module:NiFiController. Exception while disabling services: {e}")
        return False

def empty_all_queues(headers, process_group_id):
    """
        Drop all flowfiles from queues in the process group.
    """
    logger.info(f"Module:NiFiController. Emptying all queues for process group {process_group_id}.")
    try:
        connections_response = requests.get(
            f"{config.nifi.get_api_url()}/process-groups/{process_group_id}/connections",
            headers=headers, verify=False
        )

        if connections_response.status_code != 200:
            logger.error(f"Module:NiFiController. Failed to retrieve connections: {connections_response.text}")
            return False

        connections = connections_response.json()['connections']
        for conn in connections:
            conn_id = conn['id']
            drop_request = requests.post(
                f"{config.nifi.get_api_url()}/flowfile-queues/{conn_id}/drop-requests",
                headers=headers, verify=False
            )

            if drop_request.status_code != 202:
                logger.error(f"Module:NiFiController. Failed to initiate drop request for connection {conn_id}: {drop_request.text}")
                return False

        logger.info(f"Module:NiFiController. All queues emptied successfully.")
        return True

    except Exception as e:
        logger.error(f"Module:NiFiController. Exception while emptying queues: {e}")
        return False

def delete_process_group(headers, process_group_id):
    """
        Delete the process group.
    """
    logger.info(f"Module:NiFiController. Deleting process group {process_group_id}.")
    try:
        # Fetch current revision/version
        get_response = requests.get(
            f"{config.nifi.get_api_url()}/process-groups/{process_group_id}",
            headers=headers, verify=False
        )

        if get_response.status_code != 200:
            logger.error(f"Module:NiFiController. Failed to retrieve process group: {get_response.text}")
            return False

        revision = get_response.json()['revision']

        delete_response = requests.delete(
            f"{config.nifi.get_api_url()}/process-groups/{process_group_id}",
            params={"version": revision['version']},
            headers=headers, verify=False
        )

        if delete_response.status_code == 200:
            logger.info(f"Module:NiFiController. Process group {process_group_id} deleted successfully.")
            return True
        else:
            logger.error(f"Module:NiFiController. Failed to delete process group: {delete_response.text}")
            return False

    except Exception as e:
        logger.error(f"Module:NiFiController. Exception while deleting process group: {e}")
        return False

def is_ingestion_complete(headers, process_group_id):
    """
        Check if ingestion is complete by verifying: All connections have zero flowfiles queued.
    """
    logger.info(f"Module:NiFiController. Checking if ingestion is complete for process group {process_group_id}.")
    try:
        status_resp = requests.get(
            f"{config.nifi.get_api_url()}/flow/process-groups/{process_group_id}/status",
            headers=headers, verify=False
        )

        if status_resp.status_code != 200:
            logger.error(f"Module:NiFiController. Failed to get process group status: {status_resp.text}")
            return False

        status_snapshot = status_resp.json()['processGroupStatus']['aggregateSnapshot']
        queued_count = status_snapshot.get('flowFilesQueued', "0")

        if int(queued_count) > 0:
            return False

        logger.info(f"Module:NiFiController. Ingestion is complete for process group {process_group_id}.")
        return True

    except Exception as e:
        logger.error(f"Module:NiFiController. Exception while checking ingestion status: {e}")
        return False

def start_the_process(headers, process_group_id):
    """
        Run all processors within the template.
    """
    logger.info(f"Module:NiFiController. Starting the NiFi process.")
    try:
        start_payload = {"id": process_group_id, "state": "RUNNING"}
        start_response = requests.put(f"{config.nifi.get_api_url()}/flow/process-groups/{process_group_id}", 
                                    json=start_payload, headers=headers, verify=False)
        
        if start_response.status_code == 200:
            logger.info("Module:NiFiController. Flow started successfully!")
            return True
        else:
            logger.error(f"Module:NiFiController. Failed to start flow: {start_response.text}")
            return False
    except Exception as e:
        logger.error(f"Module:NiFiController. Failed to start flow: {e}")
        return False

def monitor_and_cleanup_process(headers, process_group_id, connection:Connection):
    """
        Continuously monitor the process, and automatically delete it once it completes successfully.
    """
    logger.info(f"Module:NiFiController. Started monitoring the process: {process_group_id}")
    try:
        while True:
            if is_ingestion_complete(headers, process_group_id):
                if stop_all_processors(headers, process_group_id):
                    if stop_all_services(headers, process_group_id):
                        if empty_all_queues(headers, process_group_id):
                            if delete_process_group(headers, process_group_id):
                                # Update database record
                                connection.update_state("Loaded")
                                logger.info(f"Module:NiFiController. NiFi flow {process_group_id} completed and cleaned up.")
                                store_data_kafka_to_hadoop(connection)
                                break
                            else:
                                connection.update_state("Failed")
                                break
                        else:
                            connection.update_state("Failed")
                            break
                    else:
                        connection.update_state("Failed")
                        break
                else:
                    connection.update_state("Failed")
                    break
            time.sleep(10)  # Polling interval
    except Exception as e:
        connection.update_state("Failed")
        logger.error(f"Module:NiFiController. Error in background cleanup task: {str(e)}")

def remove_failed_template(headers, process_group_id, stage):
    """
        Remove the failed template from the workspace based on the error stage.
    """
    logger.info(f"Module:NiFiController. Started removing the faulty process: {process_group_id}")
    try:
        if stage > 2:
            stop_all_processors(headers, process_group_id)
        if stage > 1:
            stop_all_services(headers, process_group_id)
            empty_all_queues(headers, process_group_id)

        deleted = delete_process_group(headers, process_group_id)
        if deleted:
            logger.info(f"Module:NiFiController. NiFi flow {process_group_id} deleted successfully.")

    except Exception as e:
        logger.error(f"Module:NiFiController. Error in removing the faulty process: {str(e)}")

def instantiate_flow(connection:Connection):
    token = get_nifi_token()
    headers = get_request_headers(token)
    process_group_id = create_template_instance(headers, connection.source_type)
    if process_group_id:
        payload = get_payload_from_variable_registry(headers, process_group_id)
        if payload:
            update_payload = set_payload_on_variable_registry(payload, connection)
            if update_payload:
                variable_updated = update_template_variable_registry(headers, process_group_id, update_payload)
                if variable_updated:
                    all_service_is_enabled = enable_controller_services_of_template(headers, process_group_id)
                    if all_service_is_enabled:
                        process_started = start_the_process(headers, process_group_id)
                        if process_started:
                            connection.nifi_process_id = process_group_id
                            connection.state = "Loading"
                            new_conn = connection.save()
                            if new_conn:
                                threading.Thread(
                                    target=monitor_and_cleanup_process,
                                    args=(headers, process_group_id, new_conn),
                                    daemon=True
                                ).start()
                                return True
                            else:
                                connection.update_state("Failed")
                                remove_failed_template(headers, process_group_id, stage=3)
                                return False
                        else:
                            connection.update_state("Failed")
                            remove_failed_template(headers, process_group_id, stage=2)
                            return False
                    else:
                        connection.update_state("Failed")
                        logger.error(f"Failed to start flow because all controller is not enabled.")
                        remove_failed_template(headers, process_group_id, stage=2)
                        return False
                else:
                    connection.update_state("Failed")
                    remove_failed_template(headers, process_group_id, stage=1)
                    return False
            else:
                connection.update_state("Failed")
                remove_failed_template(headers, process_group_id, stage=1)
                return False
        else:
            connection.update_state("Failed")
            remove_failed_template(headers, process_group_id, stage=1)
            return False
    else:
        connection.update_state("Failed")
        remove_failed_template(headers, process_group_id, stage=1)
        return False
                    

