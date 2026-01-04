"""
全局配置管理模块

从密钥管理器获取配置信息，提供全局常量和配置类，统一管理项目的配置参数
"""
import time
import datetime
from pytz import timezone
from modules.secret_manager import get_secret

sec_global_name = "global"
LOCAL_ZONE = timezone("Asia/Shanghai")


class ConfigGlobal:
    secret_dict = get_secret(sec_global_name)[1]
    env = secret_dict.get("env")
    region = secret_dict.get("region")
    athena_results_s3 = secret_dict.get("athena_result")
    redshift_user_cred = secret_dict.get("redshift_user_cred")
    redshift_iam_role = secret_dict.get("redshift_iam_role")
    smtp_connection = secret_dict.get("smtp_conn_cred")
    redshift_db_nm = secret_dict.get("redshift_database_nm")
    redshift_cluster_id = secret_dict.get("redshift_cluster_id")
