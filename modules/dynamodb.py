"""
DynamoDB 操作模块

提供 DynamoDB 表的操作功能，包括获取实体配置、更新代理表、获取表列、创建/更新项目、获取 Glue 偏移量加载 ID 等
"""
import time

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from modules.client import client, logger
from modules.conf import ConfigGlobal

dynamo_resource = boto3.resource('dynamodb', region_name="cn-north-1")
dynamo_client = client("dynamodb", 3)
LOG = logger()


def get_entity_config(domain, entity, table=ConfigGlobal.dynamo_conf_tb_name):
    resp = dynamo_resource.Table(table).query(
        KeyConditionExpression=Key("domain").eq(domain) & Key("entity").eq(entity)
    )
    res_len = resp.get("Items").__len__()
    assert res_len == 1, f"Exception: {domain} {entity} got {res_len} configuration rows."
    return resp.get("Items")[0]


def update_sfn_broker_tb(args: list):
    assert len(ConfigGlobal.dynamo_broker_tb_schema) == len(args), "Broker params length not equal with broker table"
    msg_dict = dict(zip(ConfigGlobal.dynamo_broker_tb_schema, args))
    dynamo_resource.Table(ConfigGlobal.dynamo_broker_tb_name).put_item(Item=msg_dict)


def get_table_cols(tb_name):
    tb_meta = dynamo_client.describe_table(TableName=tb_name)
    return [col.get("AttributeName") for col in tb_meta.get("Table").get("AttributeDefinitions")] if tb_meta.get(
        "Table") else None


def create_or_update_item_func(tb_name, item_dicts):
    if isinstance(item_dicts, dict):
        dynamo_resource.Table(tb_name).put_item(Item=item_dicts)
    elif isinstance(item_dicts, list):
        for item in item_dicts:
            dynamo_resource.Table(tb_name).put_item(Item=item)
    else:
        raise TypeError(f"item_dicts's type must be one of list and dict instead of {type(item_dicts)}")


def get_glue_offset_load_id(entity_name, layer):
    """
    :param entity_name: table entity name
    :param layer: layer name like raw enriched
    :return:  load_id string
    """
    retry_times = 5
    dynamodb_tb_resource = dynamo_resource.Table(ConfigGlobal.dynamo_log_tb_name)
    condition = Key("entity").eq(entity_name) & Key("job_run_status").eq("succeeded") & Key("layer").eq(layer)
    while retry_times > 0:
        try:
            scan_res = dynamodb_tb_resource.scan(ProjectionExpression="load_id", Select="SPECIFIC_ATTRIBUTES",
                                                 FilterExpression=condition).get(
                "Items", [])
            response = [item.get('load_id') for item in scan_res]
            return "19700101000000" if not response else sorted(response)[-1]
        except Exception:
            time.sleep(3)
            retry_times -= 1
    raise ClientError({
        'Error': {
            'Code': 'DynamoDBScanException',
            'Message': 'Reached max retry times'
        }
    }, 'dynamodb scan')
