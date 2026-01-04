"""
AWS Secrets Manager 操作模块

提供从 AWS Secrets Manager 获取密钥的功能，支持字符串和二进制类型的密钥
"""
import base64
import json
from modules.client import client


def get_secret(secret_name):
    secret_client = client('secretsmanager')
    get_secret_value_response = secret_client.get_secret_value(
        SecretId=secret_name
    )
    secret_arn = get_secret_value_response['ARN']
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    return secret_arn, json.loads(secret)
