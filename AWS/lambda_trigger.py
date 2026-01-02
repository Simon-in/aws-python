import datetime
import json
import os

import boto3
from boto3.dynamodb.conditions import Key


def lambda_handler(event, context):
    region = os.environ['AWS_REGION']
    sfn_client = boto3.client("stepfunctions", region_name=region)
    dynamo_resource = boto3.resource('dynamodb', region_name=region)
    load_id = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    state_machine_name = event.get("state_machine_name")
    task_token = event.get("TaskToken")
    state_machines = sfn_client.list_state_machines(maxResults=1000)
    sta_mach = [st for st in state_machines.get("stateMachines") if st["name"] == state_machine_name]
    if not sta_mach:
        raise Exception(f"No matched state machine found: {state_machine_name}")
    state_machine_arn = sta_mach[0]["stateMachineArn"]
    ph_cdp_glue_config_table = "ph-cdp-glue-job-config-table"
    dynamodb_tb_resource = dynamo_resource.Table(ph_cdp_glue_config_table)
    condition = Key("state_machine_name").eq(state_machine_name)
    related_etl_jobs = [dict(zip(["domain", "entity", "load_id"], (item.get('domain'), item.get('entity'), load_id)))
                        for item in dynamodb_tb_resource.scan(FilterExpression=condition).get("Items")]
    sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=f"{load_id}-{state_machine_name}",
        input=json.dumps({
            "task_token": task_token,
            "load_id": load_id,
            "state_machine_params": related_etl_jobs,
            "execution_name": f"{load_id}-{state_machine_name}",
            "state_machine_name": state_machine_name
        })
    )