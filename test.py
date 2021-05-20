#!/usr/bin/python3

import azkaban.api
import sys
from requests.exceptions import RequestException

def main():
    azkaban_hostname = "dc1-kdp-dev-test-01.dev.dc1.kelkoo.net"
    azkaban_port = "8081"
    azkaban_username = "test"
    azkaban_password = "test"

    status = 0
    try:
        session_id = azkaban.api.authenticate(azkaban_hostname, azkaban_port, azkaban_username, azkaban_password)
        print(f'session_id: {session_id}')

        response = azkaban.api.create_project(azkaban_hostname, azkaban_port, session_id, 'toto')
        print(response)

#        azkaban.api.delete_project(azkaban_hostname, azkaban_port, session_id, 'toto')

        project_flows = azkaban.api.fetch_project_flows(azkaban_hostname, azkaban_port, session_id, 'example_project_with_api_flow_1_0')
#        print(project_flows)

#        for flow in project_flows['flows']:
#            response = azkaban.api.fetch_schedule(azkaban_hostname, azkaban_port, session_id, flow['flowId'], project_flows['projectId'])
#            print(f"--> {flow['flowId']}: {response}")

        project_id = azkaban.api.get_project_id(azkaban_hostname, azkaban_port, session_id, 'example_project_with_api_flow_1_0')

        flows_id = azkaban.api.get_flows_id(azkaban_hostname, azkaban_port, session_id, 'example_project_with_api_flow_1_0')
#        print(flows_id)
#
#        for flow_id in flows_id:
#            flow_jobs = azkaban.api.fetch_flow_jobs(azkaban_hostname, azkaban_port, session_id, 'example_project_with_api_flow_1_0', flow_id)
#            print(flow_jobs)
#
#            schedule_id = azkaban.api.get_flow_schedule_id(azkaban_hostname, azkaban_port, session_id, flow_id, project_id)
#            print(f'{flow_id}: {schedule_id}')
#            if schedule_id is not None:
#                response = azkaban.api.unschedule_flow(azkaban_hostname, azkaban_port, session_id, schedule_id)
#                print(response)
#
#        response = azkaban.api.schedule_cron_flow(azkaban_hostname, azkaban_port, session_id, 'example_project_with_api_flow_1_0', 'example_flow_with_country_param_it', '50 1 ? * *')
#        print(response)
#
        azkaban.api.upload_project(azkaban_hostname, azkaban_port, session_id, 'toto', '/home/preaudc/dev/git/azkaban-utils/basicFlow20Project.zip')
    except RequestException as e:
        status = 1
        print(e)

    sys.exit(status)

main()
