"""
AWS 客户端和日志管理模块

提供 AWS 客户端生成和日志记录功能，包含重试机制，确保服务调用的可靠性
"""
import logging
import sys
from itertools import islice
from time import sleep

import boto3
from botocore.config import Config


def client(service_nm, max_attempts=3, timeout=5, region="cn-north-1", mode="adaptive"):
    adaptive_retries = Config(retries={
        "max_attempts": max_attempts,
        "mode": mode
    }, connect_timeout=timeout)
    return boto3.client(service_nm, region_name=region, config=adaptive_retries)


def logger():
    """
    generate new logging object to log events
    :return: logging object
    """
    logger_obj = logging.getLogger()
    for handler in logger_obj.handlers:
        logger_obj.removeHandler(handler)
    logger_obj.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s: %(levelname)s : %(message)s')
    ch.setFormatter(formatter)
    logger_obj.addHandler(ch)
    return logger_obj
