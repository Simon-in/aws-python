"""
Salesforce API 操作模块

提供连接Salesforce、批量提取数据等功能
"""
import logging
import os
import tempfile
from typing import Dict, List, Optional, Any
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from modules.secret_manager import get_secret
from modules.s3 import s3_upload
from modules.client import logger

LOG = logger()

__all__ = [
    "SalesforceError",
    "SalesforceAuthError",
    "SalesforceConnection",
    "connect_salesforce",
    "bulk_extract",
    "_delimiter_char"
]


class SalesforceError(Exception):
    """Salesforce 操作异常"""
    pass


class SalesforceAuthError(SalesforceError):
    """Salesforce 认证异常"""
    pass


class SalesforceConnection:
    """Salesforce 连接类"""
    
    def __init__(self, secret_name: str):
        """
        初始化Salesforce连接
        
        Args:
            secret_name: 密钥管理器中存储的Salesforce配置密钥名称
        """
        self.secret_name = secret_name
        self.session = requests.Session()
        self.instance_url = None
        self.access_token = None
        self.authenticate()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        retry=retry_if_exception_type(SalesforceAuthError)
    )
    def authenticate(self):
        """
        认证Salesforce连接
        """
        try:
            error, secret_dict = get_secret(self.secret_name)
            if error:
                raise SalesforceAuthError(f"获取密钥失败: {error}")
            
            # 获取认证信息
            client_id = secret_dict.get("client_id")
            client_secret = secret_dict.get("client_secret")
            username = secret_dict.get("username")
            password = secret_dict.get("password")
            security_token = secret_dict.get("security_token")
            login_url = secret_dict.get("login_url", "https://login.salesforce.com")
            
            # 检查必要参数
            missing_params = []
            for param, value in {
                "client_id": client_id,
                "client_secret": client_secret,
                "username": username,
                "password": password
            }.items():
                if not value:
                    missing_params.append(param)
            
            if missing_params:
                raise SalesforceAuthError(f"缺少必要的认证参数: {', '.join(missing_params)}")
            
            # 构建认证请求
            auth_url = f"{login_url}/services/oauth2/token"
            auth_data = {
                "grant_type": "password",
                "client_id": client_id,
                "client_secret": client_secret,
                "username": username,
                "password": f"{password}{security_token}" if security_token else password
            }
            
            # 发送认证请求
            response = requests.post(auth_url, data=auth_data)
            response.raise_for_status()
            
            # 解析认证响应
            auth_result = response.json()
            self.access_token = auth_result.get("access_token")
            self.instance_url = auth_result.get("instance_url")
            
            # 设置请求头
            self.session.headers.update({
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            })
            
            LOG.info(f"成功连接到Salesforce实例: {self.instance_url}")
            
        except requests.RequestException as e:
            raise SalesforceAuthError(f"Salesforce认证失败: {e}")
    
    def query(self, soql: str) -> List[Dict[str, Any]]:
        """
        执行SOQL查询
        
        Args:
            soql: SOQL查询语句
        
        Returns:
            查询结果列表
        """
        try:
            query_url = f"{self.instance_url}/services/data/v56.0/query"
            params = {"q": soql}
            
            all_records = []
            while True:
                response = self.session.get(query_url, params=params)
                response.raise_for_status()
                result = response.json()
                
                all_records.extend(result.get("records", []))
                
                # 处理分页
                if not result.get("nextRecordsUrl"):
                    break
                
                query_url = f"{self.instance_url}{result['nextRecordsUrl']}"
                params = {}
            
            LOG.info(f"SOQL查询成功，返回 {len(all_records)} 条记录")
            return all_records
            
        except requests.RequestException as e:
            raise SalesforceError(f"SOQL查询失败: {e}")
    
    def query_all(self, object_name: str, fields: List[str], where_clause: str = None) -> List[Dict[str, Any]]:
        """
        查询指定对象的所有记录
        
        Args:
            object_name: Salesforce对象名称
            fields: 要查询的字段列表
            where_clause: WHERE子句（可选）
        
        Returns:
            查询结果列表
        """
        fields_str = ", ".join(fields)
        soql = f"SELECT {fields_str} FROM {object_name}"
        
        if where_clause:
            soql += f" WHERE {where_clause}"
        
        return self.query(soql)
    
    def get_object_metadata(self, object_name: str) -> Dict[str, Any]:
        """
        获取对象元数据
        
        Args:
            object_name: Salesforce对象名称
        
        Returns:
            对象元数据
        """
        try:
            metadata_url = f"{self.instance_url}/services/data/v56.0/sobjects/{object_name}/describe"
            response = self.session.get(metadata_url)
            response.raise_for_status()
            
            LOG.info(f"成功获取 {object_name} 对象元数据")
            return response.json()
            
        except requests.RequestException as e:
            raise SalesforceError(f"获取对象元数据失败: {e}")


def connect_salesforce(secret_name: str) -> SalesforceConnection:
    """
    创建并返回Salesforce连接实例
    
    Args:
        secret_name: 密钥管理器中存储的Salesforce配置密钥名称
    
    Returns:
        SalesforceConnection实例
    """
    return SalesforceConnection(secret_name)


def bulk_extract(
    sf: SalesforceConnection,
    object_name: str,
    query: str,
    s3_path: str,
    column_delimiter: str = ",",
    file_format: str = "csv"
) -> None:
    """
    批量提取Salesforce数据并上传到S3
    
    Args:
        sf: SalesforceConnection实例
        object_name: Salesforce对象名称
        query: SOQL查询语句
        s3_path: S3目标路径
        column_delimiter: 列分隔符
        file_format: 文件格式（目前仅支持csv）
    """
    try:
        LOG.info(f"开始批量提取 {object_name} 数据")
        
        # 执行查询
        records = sf.query(query)
        
        if not records:
            LOG.info(f"没有找到 {object_name} 的记录")
            return
        
        # 创建临时文件
        with tempfile.NamedTemporaryFile(mode='w', suffix=f'.{file_format}', delete=False) as tmp_file:
            # 获取字段名
            fields = list(records[0].keys())
            # 过滤掉系统字段
            fields = [f for f in fields if not f.startswith("attributes")]
            
            # 写入表头
            tmp_file.write(column_delimiter.join(fields) + '\n')
            
            # 写入数据
            for record in records:
                # 过滤掉系统字段
                data = {k: v for k, v in record.items() if not k.startswith("attributes")}
                # 处理数据类型
                row = []
                for field in fields:
                    value = data.get(field, "")
                    # 如果是字符串，处理特殊字符
                    if isinstance(value, str):
                        # 处理包含分隔符的字符串，添加引号
                        if column_delimiter in value:
                            value = f'"{value.replace("\"", "\"\"")}"'
                    # 如果是None，转换为空字符串
                    elif value is None:
                        value = ""
                    # 转换为字符串
                    row.append(str(value))
                # 写入行
                tmp_file.write(column_delimiter.join(row) + '\n')
            
            tmp_file_path = tmp_file.name
        
        try:
            # 上传到S3
            s3_upload(src_path=tmp_file_path, dst_path=s3_path)
            LOG.info(f"成功将 {object_name} 数据上传到 S3: {s3_path}")
        finally:
            # 删除临时文件
            os.unlink(tmp_file_path)
    
    except Exception as e:
        raise SalesforceError(f"批量提取失败: {e}")


class _delimiter_char:
    """分隔符常量类"""
    COMMA = ","
    TAB = "\t"
    PIPE = "|"
    SEMICOLON = ";"