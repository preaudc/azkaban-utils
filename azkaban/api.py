import json
import requests
from os.path import basename
from requests.exceptions import RequestException

def authenticate(server_host, server_port, user_name, password):
    parameters = {
        'action': 'login',
        'username': user_name,
        'password': password
    }
    azkaban_url = f'http://{server_host}:{server_port}'
    r = requests.post(azkaban_url, data=parameters)
    return _get_json_from_response(r)['session.id']

def create_project(server_host, server_port, session_id, project_name):
    parameters = {
        'action': 'create',
        'session.id': session_id,
        'name': project_name,
        'description': f'{project_name}_project'
    }
    azkaban_url = f'http://{server_host}:{server_port}/manager'
    r = requests.post(azkaban_url, params=parameters)
    return _get_json_from_response(r)

def delete_project(server_host, server_port, session_id, project_name):
    parameters = {
        'delete': 'true',
        'session.id': session_id,
        'project': project_name
    }
    azkaban_url = f'http://{server_host}:{server_port}/manager'
    _call_ws_get(azkaban_url, parameters, return_json=False)

def fetch_flow_jobs(server_host, server_port, session_id, project_name, flow_name):
    parameters = {
        'ajax': 'fetchflowgraph',
        'session.id': session_id,
        'project': project_name,
        'flow': flow_name
    }
    azkaban_url = f'http://{server_host}:{server_port}/manager'
    return _call_ws_get(azkaban_url, parameters)

def fetch_project_flows(server_host, server_port, session_id, project_name):
    parameters = {
        'ajax': 'fetchprojectflows',
        'session.id': session_id,
        'project': project_name
    }
    azkaban_url = f'http://{server_host}:{server_port}/manager'
    return _call_ws_get(azkaban_url, parameters)

def fetch_schedule(server_host, server_port, session_id, flow_id, project_id):
    parameters = {
        'ajax': 'fetchSchedule',
        'session.id': session_id,
        'flowId': flow_id,
        'projectId': project_id
    }
    azkaban_url = f'http://{server_host}:{server_port}/schedule'
    return _call_ws_get(azkaban_url, parameters)

def get_flows_id(server_host, server_port, session_id, project_name):
    flows = fetch_project_flows(server_host, server_port, session_id, project_name)
    return list(map(lambda flow: flow['flowId'], flows['flows']))

def get_flow_schedule_id(server_host, server_port, session_id, flow_id, project_id):
    schedule = fetch_schedule(server_host, server_port, session_id, flow_id, project_id)
    if 'schedule' in schedule:
        return schedule['schedule']['scheduleId']

def get_project_id(server_host, server_port, session_id, project_name):
    flows = fetch_project_flows(server_host, server_port, session_id, project_name)
    if 'projectId' in flows:
        return flows['projectId']

def schedule_cron_flow(server_host, server_port, session_id, project_name, flow_name, cron_expression):
    parameters = {
        'ajax': 'scheduleCronFlow',
        'session.id': session_id,
        'projectName': project_name,
        'flow': flow_name,
        'failureAction': 'finishPossible',
        'cronExpression': f'0 {cron_expression}'
    }
    azkaban_url = f'http://{server_host}:{server_port}/schedule'
    r = requests.post(azkaban_url, params=parameters)
    return _get_json_from_response(r)

def unschedule_flow(server_host, server_port, session_id, schedule_id):
    parameters = {
        'action': 'removeSched',
        'session.id': session_id,
        'scheduleId': schedule_id
    }
    azkaban_url = f'http://{server_host}:{server_port}/schedule'
    r = requests.post(azkaban_url, params=parameters)
    return _get_json_from_response(r)

def upload_project(server_host, server_port, session_id, project_name, zip_file):
    azkaban_url = f'http://{server_host}:{server_port}/manager'
    files = {
        'file': (basename(zip_file), open(zip_file, 'rb'), 'application/zip')
    }
    parameters = {
        'ajax': 'upload',
        'session.id': session_id,
        'project': project_name
    }
    r = requests.post(azkaban_url, files=files, data=parameters)
    r.raise_for_status()
    flows_id = get_flows_id(server_host, server_port, session_id, project_name)
    if len(flows_id) == 0:
        raise RequestException(f"AzkabanError: Project '{project_name}' push on Azkaban failed")

def _call_ws_get(azkaban_url, parameters, return_json=True):
    r = requests.get(azkaban_url, params=parameters)
    r.raise_for_status()
    if return_json:
        json_response = r.json()
        if 'error' in json_response:
            raise RequestException(f"AzkabanError: {json_response['error']}")
        else:
            return r.json()
    
def _get_json_from_response(r):
    r.raise_for_status()
    json_response = r.json()
    if 'status' in json_response and json_response['status'] == 'success':
        return json_response
    else:
        if 'error' in json_response:
            raise RequestException(f"AzkabanError: {json_response['error']}")
        elif 'message' in json_response:
            raise RequestException(f"AzkabanError: {json_response['message']}")
        else:
            raise RequestException(f"AzkabanError: {json_response}")
