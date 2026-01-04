"""
Snowflake 数据库操作模块

提供 Snowflake 数据库的连接和操作功能，支持密码和私钥两种认证方式，包含执行查询、获取 stage 位置等功能
"""
import time
import base64
from typing import Union, List, Dict

from modules.client import logger
from modules.secret_manager import get_secret

from snowflake import connector as sf_conn
from snowflake.connector import SnowflakeConnection, DictCursor
from snowflake.connector.errors import ProgrammingError

LOG = logger()


class Snowflake:
    def __init__(self, username: str, password: str, account: str, database: str, schema: str, warehouse: str, role: str, private_key: str, private_key_pass: str):
        self.username = username
        self.password = password
        self.account = account
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.role = role
        self.conn = None
        self.cursor = None
        self.private_key = private_key
        self.private_key_pass = private_key_pass

    def connect(self):
        try:
            conn_params = {
                "user": self.username,
                "account": self.account,
                "database": self.database,
                "schema": self.schema,
                "warehouse": self.warehouse,
                "role": self.role,
            }
            if self.private_key and self.private_key_pass:
                conn_params.update({
                    "private_key": self.private_key,
                    "private_key_pass": self.private_key_pass,
                })
            elif self.password:
                conn_params.update({
                    "password": self.password,
                })
            else:
                raise ValueError("Either password or private key and passphrase must be provided.")
            self.conn = sf_conn.connect(**conn_params)
        except ProgrammingError as e:
            LOG.error(f"Failed to connect to Snowflake: {e}")
            raise e
        self.cursor = self.conn.cursor(DictCursor)


    def get_conn(self) -> SnowflakeConnection:
        return self.conn
    
    def close_conn(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    @classmethod
    def from_config(cls, secret_name: str, password: str = None, private_key: str = None, private_key_pass: str = None):
        _, secrets = get_secret(secret_name)
        private_key = base64.b64decode(private_key or secrets["private_key"])
        return cls(
            username=secrets["username"],
            password=password or secrets["password"],
            account=secrets["account"],
            database=secrets["database"],
            schema=secrets["schema"],
            warehouse=secrets["warehouse"],
            role=secrets["role"],
            private_key=private_key,
            private_key_pass=private_key_pass or secrets["private_key_pass"],
        )

    def execute(self, query: str):
        try:
            conn = self.get_conn()
            cur = conn.cursor(DictCursor)
            cur.execute(query)
            return cur
        except ProgrammingError as e:
            LOG.error(f"Failed to execute query: {e}")
            raise e

    def get_stage_location(self, stage_name: str):
        get_stage_location = f"select get_stage_location(@{stage_name})"
        stage_info = self.execute(get_stage_location).fetchone()
        if stage_info:
            stage_name = list(stage_info.keys())[0]
            stage_location = list(stage_info.values())[0]
            LOG.info(f"Stage Name: {stage_name}, Stage Location: {stage_location}")
        return stage_location
