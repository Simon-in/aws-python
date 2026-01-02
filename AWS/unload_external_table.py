from datetime import datetime
from typing import Dict

from modules.client import _logger
from modules.conf import ConfigGlobal
from modules.dynamodb import dynamo_resource
from modules.glue_args import get_glue_args, OptionValue
from modules.glue import get_job_run_id
from modules.redshift import (
    redshift_insert_func,
    redshift_query_executor,
    get_rs_tb_columns
)
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from modules.s3 import s3_parser_to_bucket_prefix, check_if_exist_obj

LOG = _logger()


class Status:
    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"


class Task:
    meta_schema = ConfigGlobal.redshift_log_schema
    meta_table = ConfigGlobal.redshift_audit_log_table
    meta_columns = ConfigGlobal.redshift_audit_log_cols

    def __init__(self, args: Dict):
        self.meta_db = args["RS_DB"]
        self.cluster_id = args["REDSHIFT_CLUSTER"]
        self.job_name = args["JOB_NAME"]
        self.job_run_id = get_job_run_id(self.job_name, job_arguments=args)
        self.job_start_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        self.job_end_time = ""
        self.job_run_date = datetime.utcnow().strftime("%Y-%m-%d")
        self.status = Status.RUNNING
        self.task_type = "redshift_unload"
        self.task_name = "glue_redshift_exporter"
        self.step_name = "glue_redshift_export"

    def to_dict(self):
        return {
            "job_run_date": self.job_run_date,
            "job_name": self.job_name,
            "job_start_time": self.job_start_time,
            "job_end_time": self.job_end_time,
            "status": self.status,
            "task_type": self.task_type,
            "task_name": self.task_name,
            "step_name": self.step_name,
            "job_run_id": self.job_run_id,
        }

    def create(self):
        redshift_insert_func(
            cluster_id=self.cluster_id,
            rs_db=self.meta_db,
            rs_schema=self.meta_schema,
            table_name=self.meta_table,
            rows=[self.to_dict()],
        )

    def update_status(self):
        sql = f"""
        UPDATE {self.meta_schema}.{self.meta_table} 
        SET job_end_time = '{self.job_end_time}', status = '{self.status}'
        WHERE job_run_id = '{self.job_run_id}'
        """
        redshift_query_executor(
            cluster_id=self.cluster_id,
            db_name=self.meta_db,
            sql=sql,
        )

    def complete(self):
        self.job_end_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        self.status = Status.SUCCESS
        self.update_status()

    def fail(self):
        self.job_end_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        self.status = Status.FAILED
        self.update_status()

    def log_date(self):
        sql = f"""
               SELECT job_run_date from {self.meta_schema}.{self.meta_table} 
               WHERE job_name = '{self.job_name}' and job_run_date <> {self.job_run_date}
               ORDER BY job_run_date desc limit 1
               """
        records = redshift_query_executor(
            cluster_id=self.cluster_id,
            db_name=self.meta_db,
            sql=sql,
        )
        return records[0][0].get('stringValue')


def scan_table(ddb_table, **kwargs):
    try:
        data = []
        done = False
        start_key = None
        scan_kwargs = dict(kwargs)
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = ddb_table.scan(**scan_kwargs)
            data += response.get("Items", [])
            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None
        return data
    except ClientError as ce:
        raise ce


def scan_dynamodb_table(ied):
    dynamodb_tb_resource = dynamo_resource.Table(ConfigGlobal.dynamo_conf_tb_name)
    condition = Key("is_export_del").eq(ied)
    table_data = scan_table(dynamodb_tb_resource, FilterExpression=condition)
    return table_data


if __name__ == "__main__":
    args = get_glue_args(
        positional=[
            "JOB_NAME",
            "IED"
        ],
        optional={
            "REDSHIFT_CLUSTER": ConfigGlobal.redshift_cluster_id,
            "RS_DB": ConfigGlobal.redshift_db_nm,
            "FORMAT": "PARQUET",
            "RETRY": 6,
            "DELAY": 10,
        },
    )
    cluster_id = args["REDSHIFT_CLUSTER"]
    rs_db = args["RS_DB"]
    export_fmt = args["FORMAT"]
    retry = OptionValue.get_int(args["RETRY"])
    delay = OptionValue.get_int(args["DELAY"])
    is_export_del = args["IED"]
    task = Task(args)
    task.create()
    LOG.info(f"Init glue task: {task.job_run_id}")
    current_date = datetime.utcnow().strftime("%Y-%m-%d")
    try:
        dy_dict = scan_dynamodb_table(is_export_del)
        for item in dy_dict:
            domain = item.get("domain")
            entity = item.get("entity")
            s3_source_bucket = item.get("s3_source_bucket")
            s3_source_prefix = item.get("s3_source_prefix")
            export_query = item.get("export_query")
            del_query = item.get("del_query")
            ext_schema_nm = item.get("ext_schema_nm")
            rs_schema = item.get("rs_schema")
            s3_path = f"s3://{s3_source_bucket}{s3_source_prefix}{entity}/{current_date}/"
            sql = f"UNLOAD ($${export_query}$$) TO '{s3_path}' IAM_ROLE '{ConfigGlobal.redshift_iam_role}' FORMAT AS {export_fmt} "
            LOG.info(f"unload sql: {sql}")
            redshift_query_executor(cluster_id, rs_db, sql, retry=retry, delay=delay)
            bucket, prefix, file_nm = s3_parser_to_bucket_prefix(s3_path)
            if check_if_exist_obj(bucket, prefix):
                select_ext_table = f"SELECT * FROM SVV_EXTERNAL_TABLES WHERE TABLENAME = '{entity}_external'"
                cnt = redshift_query_executor(cluster_id, rs_db, select_ext_table)
                if len(cnt) > 0:
                    LOG.info(f"The {rs_schema}.{entity} Unload successfully.")
                else:
                    s3_path = f"s3://{s3_source_bucket}{s3_source_prefix}{entity}/"
                    src_cols = get_rs_tb_columns(cluster_id, rs_db, rs_schema, entity)
                    col = ",\n".join([f'"{col}" varchar(65535)' for col in src_cols])
                    create_ext_ddl = f" CREATE EXTERNAL TABLE {ext_schema_nm}.{entity}_external({col}) STORED AS parquet LOCATION '{s3_path}'"
                    redshift_query_executor(cluster_id, rs_db, create_ext_ddl)
                redshift_query_executor(cluster_id, rs_db, del_query)
            else:
                LOG.info(f"The {rs_schema}.{entity} having not data unload Skip the program!")
    except Exception as e:
        LOG.error(f"Error unload : {e}")
        task.fail()
        raise e
    else:
        task.complete()