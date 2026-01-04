# AWS Python 项目

这是一个基于 Python 的 AWS 云服务集成项目，提供了丰富的模块和工具，用于处理各种云数据集成、ETL 任务和自动化操作。

## 项目概述

该项目旨在简化 AWS 相关服务的开发和集成，提供了一系列封装良好的模块，支持：

- AWS Glue ETL 作业开发
- S3 存储操作
- Redshift 数据仓库交互
- Snowflake 数据处理
- DynamoDB 配置管理
- 邮件发送服务
- REST API 交互
- Rclone 数据同步
- Salesforce 数据集成
- 等等...

## 目录结构

```
aws-python/
├── AWS/                    # AWS 相关功能模块
│   ├── landing.py          # 数据落地作业
│   └── unload_external_table.py  # 外部表卸载
├── Dataverse/              # Dataverse 集成模块
│   ├── redshift_to_blob.py # Redshift 到 Blob 存储
│   └── redshift_to_dataverse_api.py  # Redshift 到 Dataverse API
├── SnowFlake/              # Snowflake 相关功能
│   ├── Rclone_snowflake_to_sftp.py  # Snowflake 到 SFTP 数据同步
│   └── email.py            # 邮件发送服务
├── modules/                # 核心模块库
│   ├── rclone/             # Rclone 封装
│   │   ├── __init__.py
│   │   ├── bin.py
│   │   ├── rclone.py
│   │   └── utils.py
│   ├── target_restapi/     # REST API 目标端
│   │   ├── __init__.py
│   │   ├── source_redshift.py
│   │   ├── utils.py
│   │   └── wal.py
│   ├── __init__.py
│   ├── athena.py           # Athena 服务封装
│   ├── client.py           # AWS 客户端生成器
│   ├── conf.py             # 全局配置
│   ├── dynamodb.py         # DynamoDB 处理
│   ├── email.py            # 邮件服务
│   ├── glue_args.py        # Glue 参数解析
│   ├── redshift.py         # Redshift 处理
│   ├── s3.py               # S3 操作
│   ├── salesforce.py       # Salesforce 集成
│   ├── secret_manager.py   # 密钥管理器
│   └── snowflake.py        # Snowflake 集成
└── README.md               # 项目文档
```

## 核心模块

### 1. 客户端生成器 (client.py)

封装了 AWS 客户端的创建逻辑，提供了带重试机制的客户端生成功能。

```python
from modules.client import client, _logger

# 创建带重试机制的 S3 客户端
s3_client = client("s3", max_attempts=5, timeout=10)

# 获取日志记录器
LOG = _logger()
LOG.info("这是一条日志信息")
```

### 2. 配置管理 (conf.py)

提供全局配置管理，从密钥管理器获取配置信息。

```python
from modules.conf import ConfigGlobal

# 获取环境信息
env = ConfigGlobal.env
region = ConfigGlobal.region

# 获取 Redshift 配置
redshift_db = ConfigGlobal.redshift_db_nm
```

### 3. S3 操作 (s3.py)

提供了丰富的 S3 操作功能，包括文件上传、下载、复制、删除等。

```python
from modules.s3 import s3_upload, s3_download, s3_copy_func

# 上传文件到 S3
s3_upload(local_path="/tmp/data.csv", bucket="my-bucket", key="prefix/data.csv")

# 从 S3 下载文件
s3_download(bucket="my-bucket", key="prefix/data.csv", local_path="/tmp/downloaded.csv")

# 复制 S3 文件
s3_copy_func(source_bucket="source-bucket", source_key="source/key", dest_bucket="dest-bucket", dest_key="dest/key")
```

### 4. Redshift 处理 (redshift.py)

封装了 Redshift 数据库的交互操作，包括查询执行和数据插入。

```python
from modules.redshift import redshift_query_executor, redshift_insert_func

# 执行查询
result = redshift_query_executor(
    cluster_id="my-redshift-cluster",
    db_name="my-db",
    query="SELECT * FROM table LIMIT 10"
)

# 插入数据
redshift_insert_func(
    cluster_id="my-redshift-cluster",
    db_name="my-db",
    schema="my-schema",
    columns=["col1", "col2"],
    data=[{"col1": "value1", "col2": "value2"}]
)
```

### 5. 密钥管理 (secret_manager.py)

用于从 AWS Secrets Manager 获取密钥和配置信息。

```python
from modules.secret_manager import get_secret

# 获取密钥
error, secret_dict = get_secret("my-secret")
if not error:
    username = secret_dict.get("username")
    password = secret_dict.get("password")
```

### 6. DynamoDB 处理 (dynamodb.py)

提供了 DynamoDB 表的操作功能。

```python
from modules.dynamodb import get_entity_config

# 获取实体配置
config = get_entity_config(domain="my-domain", entity="my-entity")
```

### 7. 邮件服务 (email.py)

封装了邮件发送功能。

```python
from modules.email import email_sender

# 发送邮件
email_sender(
    subject="测试邮件",
    body="这是一封测试邮件",
    to=["user@example.com"],
    attachments=["/tmp/attachment.pdf"]
)
```

### 8. Snowflake 集成 (snowflake.py)

提供了 Snowflake 数据库的连接和操作功能。

```python
from modules.snowflake import SnowflakeConnector

# 创建 Snowflake 连接
connector = SnowflakeConnector(
    account="my-account",
    user="my-user",
    password="my-password",
    warehouse="my-warehouse",
    database="my-database",
    schema="my-schema"
)

# 执行查询
result = connector.execute_query("SELECT * FROM table LIMIT 10")

# 关闭连接
connector.close()
```

### 9. Salesforce 集成 (salesforce.py)

提供了 Salesforce API 的连接和批量数据提取功能。

```python
from modules.salesforce import SalesforceHandler

# 创建 Salesforce 连接
sf_handler = SalesforceHandler(
    client_id="my-client-id",
    client_secret="my-client-secret",
    username="my-username",
    password="my-password",
    security_token="my-security-token"
)

# 批量提取数据
extracted_data = sf_handler.bulk_extract(
    object_name="Account",
    query="SELECT Id, Name, Industry FROM Account",
    batch_size=1000
)

# 获取令牌
token = sf_handler.get_token()
```

## 使用示例

### AWS 数据落地作业

```python
from AWS.landing import landing_func

# 运行数据落地作业
landing_func(spark, domain="my-domain", entity="my-entity")
```

### Snowflake 到 SFTP 数据同步

```python
from SnowFlake.Rclone_snowflake_to_sftp import sync_data

# 同步数据
 sync_data(
    source="snowflake://my-account/my-db/my-schema/my-table",
    destination="sftp://user:pass@example.com/path/",
    config={"file_format": "csv"}
 )
```

### Dataverse 集成

```python
from Dataverse.redshift_to_dataverse_api import sync_redshift_to_dataverse

# 从 Redshift 同步数据到 Dataverse
 sync_redshift_to_dataverse(
    redshift_query="SELECT * FROM my-table",
    dataverse_entity="my-entity",
    mapping={"redshift_col": "dataverse_field"}
 )
```

## 安装说明

1. 克隆项目代码

```bash
git clone https://github.com/your-username/aws-python.git
cd aws-python
```

2. 安装依赖

```bash
pip install -r requirements.txt
```

3. 配置 AWS 凭证

确保已配置 AWS 凭证，可以通过以下方式之一：

- AWS CLI 配置
- 环境变量 `AWS_ACCESS_KEY_ID` 和 `AWS_SECRET_ACCESS_KEY`
- IAM 角色（如果在 AWS 服务中运行）

## 依赖项

- boto3
- botocore
- pandas
- requests
- tenacity
- msal
- pytz
- shlex

## 配置

项目使用 AWS Secrets Manager 存储配置信息。需要创建一个名为 `global` 的密钥，包含以下字段：

- `env` - 环境名称（如 dev, prod）
- `region` - AWS 区域
- `athena_result` - Athena 查询结果存储位置
- `redshift_user_cred` - Redshift 用户凭证
- `redshift_iam_role` - Redshift IAM 角色
- `smtp_conn_cred` - SMTP 连接凭证
- `redshift_database_nm` - Redshift 数据库名称
- `redshift_cluster_id` - Redshift 集群 ID

## 贡献指南

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

## 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情

## 联系方式

如有问题或建议，请联系项目维护者。

---

**版本**: 1.1.0  
**最后更新**: 2026-01-04