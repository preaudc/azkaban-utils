import json
import requests
from urllib.parse import urlencode
from urllib.request import Request, urlopen

def authenticate(server_host, server_port, user_name, password):
    parameters = urlencode({
        'action':'login',
        'username':user_name,
        'password':password
    })
    parameters = parameters.encode('ascii')
    azkaban_url = f'http://{server_host}:{server_port}'
    with urlopen(azkaban_url, parameters) as fp:
        return json.loads(fp.read())

def create_project(server_host, server_port, session_id, project_name):
    parameters = urlencode({
        'action':'create',
        'session.id':session_id,
        'name':project_name,
        'description':f'{project_name}_project'
    })
    parameters = parameters.encode('ascii')
    azkaban_url = f'http://{server_host}:{server_port}/manager'
    with urlopen(azkaban_url, parameters) as fp:
        return json.loads(fp.read())

def delete_project(server_host, server_port, session_id, project_name):
    parameters = urlencode({
        'delete':'true',
        'session.id':session_id,
        'project':project_name
    })
    azkaban_url = f'http://{server_host}:{server_port}/manager?{parameters}'
    urlopen(azkaban_url)

def fetch_project_flows(server_host, server_port, session_id, project_name):
    parameters = urlencode({
        'ajax':'fetchprojectflows',
        'session.id':session_id,
        'project':project_name
    })
    azkaban_url = f'http://{server_host}:{server_port}/manager?{parameters}'
    with urlopen(azkaban_url) as fp:
        return json.loads(fp.read())

def fetch_schedule(server_host, server_port, session_id, flow_id, project_id):
    parameters = urlencode({
        'ajax':'fetchSchedule',
        'session.id':session_id,
        'flowId':flow_id,
        'projectId':project_id
    })
    azkaban_url = f'http://{server_host}:{server_port}/schedule?{parameters}'
    with urlopen(azkaban_url) as fp:
        return json.loads(fp.read())

def get_flows_id(server_host, server_port, session_id, project_name):
    flows = fetch_project_flows(server_host, server_port, session_id, project_name)
    return list(map(lambda flow: flow['flowId'], flows['flows']))

def get_flow_schedule_id(server_host, server_port, session_id, flow_id, project_id):
    schedule = fetch_schedule(server_host, server_port, session_id, flow_id, project_id)
    if 'schedule' in schedule:
        return schedule['schedule']['scheduleId']

def get_project_id(server_host, server_port, session_id, project_name):
    flows = fetch_project_flows(server_host, server_port, session_id, project_name)
    return flows['projectId']

def schedule_cron_flow(server_host, server_port, session_id, project_name, flow_name, cron_expression):
    parameters = urlencode({
        'ajax':'scheduleCronFlow',
        'session.id':session_id,
        'projectName':project_name,
        'flow':flow_name,
        'failureAction':'finishPossible',
        'cronExpression':f'0 {cron_expression}'
    })
    parameters = parameters.encode('ascii')
    azkaban_url = f'http://{server_host}:{server_port}/schedule'
    with urlopen(azkaban_url, parameters) as fp:
        return json.loads(fp.read())

def unschedule_flow(server_host, server_port, session_id, schedule_id):
    parameters = urlencode({
        'action':'removeSched',
        'session.id':session_id,
        'scheduleId':schedule_id
    })
    parameters = parameters.encode('ascii')
    azkaban_url = f'http://{server_host}:{server_port}/schedule'
    with urlopen(azkaban_url, parameters) as fp:
        return json.loads(fp.read())

def upload_project(server_host, server_port, session_id, project_name, zip_file):
    azkaban_url = f'http://{server_host}:{server_port}/manager'
    files = {
        'file': ('toto.zip', open(zip_file, 'rb'), 'application/zip')
    }
    parameters = {
        'ajax':'upload',
        'session.id':session_id,
        'project':project_name
    }
    r = requests.post(azkaban_url, files=files, data=parameters)
    print(r.text)

#    file_data = open(zip_file, 'rb')
#    parameters = urlencode({
#        'ajax':'upload',
#        'session.id':session_id,
#        'file':f'{file_data};type=application/zip',
#        'project':project_name
#    })
#    parameters = parameters.encode('ascii')
#    azkaban_url = f'http://{server_host}:{server_port}/manager'
#    #headers = {'Content-Type':'multipart/mixed'}
#    req = Request(url = azkaban_url, data = parameters) #, headers = headers)
#    with urlopen(req) as fp:
#        #return json.loads(fp.read())
#        print(fp.read())
