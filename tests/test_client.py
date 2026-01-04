import pytest
from unittest.mock import patch
from modules.client import client, _logger


def test_client_creation():
    """测试AWS客户端创建功能"""
    with patch('boto3.client') as mock_boto_client:
        # 调用client函数创建S3客户端
        s3_client = client("s3", max_attempts=3, timeout=5)
        
        # 验证boto3.client被正确调用
        mock_boto_client.assert_called_once()
        
        # 验证返回值不是None
        assert s3_client is not None


def test_logger_creation():
    """测试日志记录器创建功能"""
    # 调用_logger函数获取日志记录器
    logger = _logger()
    
    # 验证日志记录器创建成功
    assert logger is not None
    assert hasattr(logger, 'info')
    assert hasattr(logger, 'error')
    assert hasattr(logger, 'debug')
