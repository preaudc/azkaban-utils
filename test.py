#!/usr/bin/python3

import azkaban.api
import json
import sys
from urllib.error import URLError
from urllib.request import urlopen

def get_active_nn(nn_list):
    def is_active_nn(nn):
        jmx_url = "http://{host}:{port}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus".format(
            host = nn['Host'],
            port = nn['Port']
        )
        try:
            jsonData = json.loads(urlopen(jmx_url).read())
            return 'active' == jsonData['beans'][0]['State']
        except URLError as e:
            pass

    for nn in nn_list:
        if is_active_nn(nn):
            return nn
    sys.exit('ERROR: all NameNodes are DOWN!')

def main():
    azkaban_hostname = "localhost"
    azkaban_port = "8081"
    azkaban_username = "azkaban"
    azkaban_password = "azkaban"

    session_id=""
    response = azkaban.api.authenticate(azkaban_hostname, azkaban_port, azkaban_username, azkaban_password)
    if 'status' in response and response['status'] == 'success':
        session_id = response['session.id']
    else:
        sys.exit('ERROR: ' + response)

    print(f'session_id: {session_id}')

    response = azkaban.api.create_project(azkaban_hostname, azkaban_port, session_id, 'toto')
    print(response)

#    response = azkaban.api.delete_project(azkaban_hostname, azkaban_port, session_id, 'toto')
#    print(response)

#    project_flows = azkaban.api.fetch_project_flows(azkaban_hostname, azkaban_port, session_id, 'project_api_flow_10')
#    print(project_flows)
#
#    for flow in project_flows['flows']:
#        response = azkaban.api.fetch_schedule(azkaban_hostname, azkaban_port, session_id, flow['flowId'], project_flows['projectId'])
#        print(response)
#
#    project_id = azkaban.api.get_project_id(azkaban_hostname, azkaban_port, session_id, 'project_api_flow_10')
#
#    flows_id = azkaban.api.get_flows_id(azkaban_hostname, azkaban_port, session_id, 'project_api_flow_10')
#
#    for flow_id in flows_id:
#        schedule_id = azkaban.api.get_flow_schedule_id(azkaban_hostname, azkaban_port, session_id, flow_id, project_id)
#        print(f'{flow_id}: {schedule_id}')
#
#        if schedule_id is not None:
#            response = azkaban.api.unschedule_flow(azkaban_hostname, azkaban_port, session_id, schedule_id)
#            print(response)
#
#    response = azkaban.api.schedule_cron_flow(azkaban_hostname, azkaban_port, session_id, 'project_api_flow_10', 'flow10_country_param_it', '50 1 ? * *')
#    print(response)

    azkaban.api.upload_project(azkaban_hostname, azkaban_port, session_id, 'toto', '/home/cpreaud/dev/git/azkaban-utils/basicFlow20Project.zip')

main()
