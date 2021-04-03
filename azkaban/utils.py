import json
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

#fetchProjectFlows() {
#  local server_host="$1"
#  local server_port="$2"
#  local session_id="$3"
#  local project_name="$4"
#  curl --silent --get \
#    --data "ajax=fetchprojectflows&session.id=${session_id}&project=${project_name}" \
#    http://${server_host}:${server_port}/manager
#}
#
#fetchSchedule() {
#  local server_host="$1"
#  local server_port="$2"
#  local session_id="$3"
#  local flow_id="$4"
#  local project_id="$5"
#  curl --silent --get \
#    --data "ajax=fetchSchedule&session.id=${session_id}&flowId=${flow_id}&projectId=${project_id}" \
#    http://${server_host}:${server_port}/schedule
#}
#
#getFlowsId() {
#  local server_host="$1"
#  local server_port="$2"
#  local session_id="$3"
#  local project_name="$4"
#  (fetchProjectFlows ${server_host} ${server_port} ${session_id} ${project_name}) | jq --raw-output '.flows[].flowId'
#}
#
#getFlowScheduleId() {
#  local server_host="$1"
#  local server_port="$2"
#  local session_id="$3"
#  local flow_id="$4"
#  local project_id="$5"
#  (fetchSchedule ${server_host} ${server_port} ${session_id} ${flow_id} ${project_id}) | jq --raw-output '.schedule.scheduleId'
#}
#
#getProjectId() {
#  local server_host="$1"
#  local server_port="$2"
#  local session_id="$3"
#  local project_name="$4"
#  (fetchProjectFlows ${server_host} ${server_port} ${session_id} ${project_name}) | jq --raw-output '.projectId'
#}
#
#scheduleCronFlow() {
#  local server_host="$1"
#  local server_port="$2"
#  local session_id="$3"
#  local project_name="$4"
#  local flow_name="$5"
#  local cron_expression="$6"
#  curl --silent --get \
#    --data "ajax=scheduleCronFlow&session.id=${session_id}&projectName=${project_name}&flow=${flow_name}&failureAction=finishPossible" \
#    --data-urlencode cronExpression="0 ${cron_expression}" \
#    http://${server_host}:${server_port}/schedule
#}
#
#unscheduleFlow() {
#  local server_host="$1"
#  local server_port="$2"
#  local session_id="$3"
#  local schedule_id="$4"
#  curl --silent --request POST \
#    --data "action=removeSched&session.id=${session_id}&scheduleId=$schedule_id" \
#    http://${server_host}:${server_port}/schedule
#}
#
#uploadProject() {
#  local server_host="$1"
#  local server_port="$2"
#  local session_id="$3"
#  local project_name="$4"
#  local zip_file="$5"
#  curl --silent --request POST \
#    --form "session.id=${session_id}" \
#    --form "ajax=upload" \
#    --form "file=@${zip_file};type=application/zip" \
#    --form "project=${project_name}" \
#    http://${server_host}:${server_port}/manager
#}
