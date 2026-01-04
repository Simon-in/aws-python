"""
Redshift 数据库操作模块

提供 Redshift 数据库的各种操作功能，包括查询执行、数据卸载、表操作等，集成了 S3 数据交互
"""
import datetime
import json
import os.path
import textwrap
import uuid
from typing import List, Dict

import boto3
import redo
from botocore.exceptions import WaiterError
from botocore.waiter import WaiterModel, Waiter
from botocore.waiter import create_waiter_with_client
from modules.client import client, logger
from modules.conf import ConfigGlobal
from modules.s3 import (
    s3_parser_to_bucket_prefix,
    list_s3_objs,
    s3_source_csvs_to_excel,
    is_s3_path_empty,
    s3_copy,
    s3_delete_folder,
    s3_delete_object,
)
from modules.secret_manager_handler import get_secret
from modules import dsl

utc_now_str = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
redshift_client = client("redshift-data")
s3_client = client("s3", 3)
user_credential_arn = get_secret(ConfigGlobal.redshift_user_cred)[0]
LOG = logger()


def execution_waiter_generator(retry, delay, waiter_name):
    waiter_config = {
        "version": 2,
        "waiters": {
            waiter_name: {
                "operation": "DescribeStatement",
                "delay": delay,
                "maxAttempts": retry,
                "acceptors": [
                    {"matcher": "path", "expected": "FINISHED", "argument": "Status", "state": "success"},
                    {"matcher": "path", "expected": "PICKED", "argument": "Status", "state": "retry"},
                    {"matcher": "path", "expected": "STARTED", "argument": "Status", "state": "retry"},
                    {"matcher": "path", "expected": "SUBMITTED", "argument": "Status", "state": "retry"},
                    {"matcher": "path", "expected": "FAILED", "argument": "Status", "state": "failure"},
                    {"matcher": "path", "expected": "ABORTED", "argument": "Status", "state": "failure"},
                ],
            },
        },
    }
    waiter_model = WaiterModel(waiter_config)
    return create_waiter_with_client(waiter_name, waiter_model, redshift_client)


def fetchone_statement_result(client, statement_id, dict_cursor=False):
    res = client.get_statement_result(Id=statement_id)
    records = res.get("Records", [])
    while res.get("NextToken"):
        res = client.get_statement_result(Id=statement_id, NextToken=res.get("NextToken"))
        records += res.get("Records", [])
    if dict_cursor:
        dict_records = []
        names = [col["name"] for col in res["ColumnMetadata"]]
        for record in records:
            values = []
            for col in record:
                col_type = list(col.keys())[0]
                col_value = list(col.values())[0]
                if col_type == "isNull":
                    col_value = None
                values.append(col_value)
            dict_records.append(dict(zip(names, values)))
        return dict_records
    return records


def fetchall_statements_result(client, statements: List[Dict]) -> Dict[str, List[Dict]]:
    """
    :statements: a list of statement descriptions
    :return: a dict of statement_id and its result
    """
    ret = dict()
    for desc in statements:
        if not desc.get("HasResultSet", False):
            continue
        sid = desc["Id"]
        status = desc["Status"]
        if status != "FINISHED":
            raise ValueError(f"Statement {sid} is not finished, status: {status}")
        records = fetchone_statement_result(client, sid)
        ret[sid] = records
    return ret


class WaiterExceedsError(WaiterError):
    """Redshift execute max attempts exceeded"""


def wait_raise_exceeds(waiter: Waiter, statement_id: str):
    """
    Waiter.wait() will raise WaiterError if it exceeds the max attempts.
    """
    try:
        waiter.wait(Id=statement_id)
    except WaiterError as e:
        if e.last_response["Status"] in ("STARTED", "SUBMITTED", "PICKED"):
            raise WaiterExceedsError(
                waiter.name,
                reason="Max attempts exceeded",
                last_response=e.last_response,
            )
        else:
            raise e


def _execute_statement(
        client,
        cluster_id,
        db_name,
        secret_arn,
        sql: str = None,
        sqls: List[str] = None,
        wait_retry=5,
        wait_delay=3,
        attempts=5,
        sleeptime=60,
        dict_cursor=False
):
    if not sql and not sqls:
        raise ValueError("sql or sqls must be provided")

    batch = True if sqls else False
    if batch:
        execution = client.batch_execute_statement(
            ClusterIdentifier=cluster_id, Database=db_name, SecretArn=secret_arn, Sqls=sqls
        )
    else:
        execution = client.execute_statement(
            ClusterIdentifier=cluster_id, Database=db_name, SecretArn=secret_arn, Sql=sql
        )

    statement_id = execution.get("Id")

    waiter = execution_waiter_generator(retry=wait_retry, delay=wait_delay, waiter_name="RedshiftStatementExecution")
    try:
        redo.retry(
            action=wait_raise_exceeds,
            attempts=attempts,
            sleeptime=sleeptime,
            args=(waiter, statement_id),
            retry_exceptions=(WaiterExceedsError,),
        )
    except WaiterError as e:
        raise e

    desc = client.describe_statement(Id=statement_id)
    if not desc.get("HasResultSet", False):
        return []

    if desc.get("SubStatements", []):
        return fetchall_statements_result(client, desc["SubStatements"])
    else:
        return fetchone_statement_result(client, statement_id, dict_cursor=dict_cursor)


#  wait time = retry * delay * attempts + attempts * sleeptime
def redshift_query_executor(cluster_id, db_name, sql, retry=5, delay=10, batch=True, **kwargs):
    if not batch:
        sqls = [sql]
    else:
        sqls = [_.strip() for _ in sql.split(";") if _.strip()]
    attempts = kwargs.get("attempts", 5)
    sleeptime = kwargs.get("sleeptime", 60)
    dict_cursor = kwargs.get("dict_cursor", False)
    secret_arn = kwargs.get("secret_arn", user_credential_arn)
    try:
        if len(sqls) > 1:
            records = _execute_statement(
                redshift_client,
                cluster_id,
                db_name,
                secret_arn,
                sqls=sqls,
                wait_retry=retry,
                wait_delay=delay,
                attempts=attempts,
                sleeptime=sleeptime,
            )
        else:
            records = _execute_statement(
                redshift_client,
                cluster_id,
                db_name,
                secret_arn,
                sql=sql,
                wait_retry=retry,
                wait_delay=delay,
                attempts=attempts,
                sleeptime=sleeptime,
                dict_cursor=dict_cursor
            )
        return records
    except WaiterError as e:
        query_resp = json.dumps(e.last_response, default=str)
        LOG.error(
            f"Redshift execution failed: {e}\tsql: {sql}\terror response: {query_resp}"
        )
        raise e


def sense_table_existence(cluster_id, rs_db, rs_schema, rs_tb):
    tb_desc_resp = redshift_client.list_tables(
        ClusterIdentifier=cluster_id,
        Database=rs_db,
        SecretArn=user_credential_arn,
        SchemaPattern=rs_schema,
        TablePattern=rs_tb
    )
    tb_list = [tb.get("name") for tb in tb_desc_resp.get("Tables")]
    return True if rs_tb in tb_list else False


def get_rs_tb_columns(cluster_id, rs_db, rs_schema, rs_tb):
    col_list = []
    tb_desc = redshift_client.describe_table(
        ClusterIdentifier=cluster_id,
        Database=rs_db,
        SecretArn=user_credential_arn,
        Schema=rs_schema,
        Table=rs_tb
    )
    col_list = col_list + tb_desc.get("ColumnList")
    while tb_desc.get("NextToken"):
        tb_desc = redshift_client.describe_table(
            ClusterIdentifier=cluster_id,
            Database=rs_db,
            SecretArn=user_credential_arn,
            Schema=rs_schema,
            Table=rs_tb,
            NextToken=tb_desc.get("NextToken")
        )
        col_list = col_list + tb_desc.get("ColumnList")
    return [col_desc.get("name") for col_desc in col_list]


def create_redshift_stage_tb(cluster_id, cols, rs_db, stage_tb, rs_schema="etl_stage"):
    """
    :param cluster_id: redshift cluster id
    :param cols: table columns
    :param rs_db: redshift name you want to create table
    :param stage_tb: redshift name you w
    :param rs_schema: redshift schema name you want to create table
    """
    column_list_expr = ",".join(['"' + col + '"' + f' varchar(max)' for col in cols])
    dynamic_ddl = f"""
    CREATE TABLE IF NOT EXISTS 
    {rs_db}.{rs_schema}.{stage_tb}
    ({column_list_expr})
    """
    update_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    ddl_log_dict = dict(zip(ConfigGlobal.redshift_ddl_history_log_cols, [stage_tb, rs_schema, dynamic_ddl,
                                                                         update_time, "", "s3_to_redshift_glue_job"]))
    try:
        redshift_query_executor(cluster_id, rs_db, dynamic_ddl)
        LOG.info(f"Stage table: {stage_tb} created successfully!")
        ddl_log_dict["status"] = "success"
    except Exception as e:
        LOG.error(f"Table {rs_schema}.{stage_tb} creation: {dynamic_ddl} failed due to {e}")
        ddl_log_dict["status"] = "failed"
        raise
    finally:
        redshift_insert_func(cluster_id, rs_db, ConfigGlobal.redshift_log_schema,
                             ConfigGlobal.redshift_ddl_history_log_table, [ddl_log_dict])



def redshift_drop_table(cluster_id, rs_db, rs_schema, rs_tb):
    drop_sql = f"DROP TABLE {rs_schema}.{rs_tb}"
    try:
        redshift_query_executor(cluster_id, rs_db, drop_sql)
    except Exception as e:
        LOG.error(f"Drop table {rs_schema}.{rs_tb}: {drop_sql} failed due to {e}")
        raise e


def redshift_truncate_table(cluster_id, rs_db, rs_schema, rs_tb):
    truncate_sql = f"TRUNCATE TABLE {rs_schema}.{rs_tb}"
    try:
        redshift_query_executor(cluster_id, rs_db, truncate_sql)
    except Exception as e:
        LOG.error(f"Truncate table {rs_schema}.{rs_tb}: {truncate_sql} failed due to {e}")
        raise e


#  will add data type validation in future 
def redshift_insert_func(cluster_id: str, rs_db: str, rs_schema: str, table_name: str, rows: list):
    """
    insert rows into redshift tables
    :param cluster_id: redshift cluster id
    :param rs_db: redshift database name
    :param rs_schema: redshift schema name
    :param table_name: redshift table name
    :param rows: values in list or dict format eg: [[val1, val2 ...], [val3, val3 ...], ...]
    :return:
    """
    cols = get_rs_tb_columns(cluster_id, rs_db, rs_schema, table_name)
    insert_sql = f"""
    INSERT INTO "{rs_schema}"."{table_name}" ({",".join(cols)}) VALUES
    """
    for row in rows:
        if type(row) == list:
            if len(row) == len(cols):
                val_fmt = [f"'{v}'" for v in row]
                row_str = ",".join(val_fmt)
                insert_sql += f"({row_str}),"
            else:
                input_val_str = ",".join(row)
                LOG.error(f"Invalid input value found: {input_val_str}! Expected value length: {len(cols)} "
                          f"but {len(row)} found!")
                raise Exception("Exception: Invalid input values.")
        elif type(row) == dict:
            if sorted(cols) == sorted(list(row.keys())):
                val_fmt = [f"'{row.get(col)}'" for col in cols]
                row_str = ",".join(val_fmt)
                insert_sql += f"({row_str}),"
            else:
                LOG.error(f"Invalid input value found: {str(row)}, incorrect schema!")
                raise Exception("Exception: Invalid input values.")
        else:
            raise Exception("Exception: Invalid format input found.")

    insert_sql = insert_sql[:-1] + ";"
    try:
        redshift_query_executor(cluster_id, rs_db, insert_sql)
    except Exception as e:
        LOG.error(f"Error occurred while execute script: \n"
                  f"*****************/{insert_sql}/*****************")
        raise e


def redshift_unload_func(
        cluster_id,
        rs_db,
        statement,
        s3_uri,
        fmt="csv",
        compress_type: str = None,
        allow_overwrite: bool = True,
        header: bool = True,
        delimiter: str = None,
        add_quotes: bool = False,
        null_as: str = None,
        escape: bool = False,
        retry=10,
        delay=10,
):
    """unload data from redshift to s3.
        * https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html
    :param cluster_id: redshift cluster id
    :param rs_db: redshift database name
    :param statement: query statement to unload data
    :param s3_uri: s3 path to store unloaded data
    :param fmt: file format of unloaded data, support CSV | XLSX | PARQUET | JSON
    :param compress_type: compression type of unloaded data, support GZIP | BZIP2 | ZSTD
    :param delimiter: delimiter of unloaded data
    :param allow_overwrite: allow to overwrite existing data in s3
    :param header: add header to unloaded data
    :param add_quotes: add quotes to unloaded data
    :param null_as: replace null value with specified value
    :param escape: escape special characters
    :param retry: redshift query retrieve status tims
    :param delay: redshift retrieve interval
    """
    fmt = fmt.upper()
    compress_type = compress_type.upper() if compress_type else None
    assert fmt in [
        "CSV",
        "PARQUET",
        "JSON",
    ], "format must be CSV | XLSX | PARQUET | JSON"
    assert compress_type in [
        "GZIP",
        "BZIP2",
        "ZSTD",
        None,
    ], "compress_type must be GZIP | BZIP2 | ZSTD"
    if fmt in ("PARQUET", "JSON"):
        if header:
            raise ValueError(f"HEADER is not supported for UNLOAD to {fmt}")
        if delimiter:
            raise ValueError(f"DELIMITER is not supported for UNLOAD to {fmt}")
        if add_quotes:
            raise ValueError(f"ADDQUOTES is not supported for UNLOAD to {fmt}")
    s3_uri = dsl.render(s3_uri)
    file_ext = os.path.splitext(s3_uri)[-1].upper().lstrip(".")
    if compress_type:
        compression_ext = {
            "GZIP": "GZ",
            "BZIP2": "BZ2",
            "ZSTD": "ZST",
        }
        if file_ext != compression_ext.get(compress_type):
            raise ValueError(
                f"file extension '{file_ext.lower()}' does not match "
                f"compression '{compression_ext.get(compress_type).lower()}'\ts3_uri: '{s3_uri}'"
            )
    else:
        if file_ext != fmt:
            raise ValueError(
                f"file extension '{file_ext.lower()}' does not match "
                f"format '{fmt.lower()}'\ts3_uri: '{s3_uri}'"
            )
    if not is_s3_path_empty(s3_uri):
        if allow_overwrite is False:
            raise ValueError(f"s3 path {s3_uri} is not empty, set allow_overwrite=True to overwrite")
    unload_temp_path = ConfigGlobal.redshift_unload_stage_s3 + str(uuid.uuid4()) + "/"
    format_exp = f"FORMAT AS {fmt}"
    header_exp = "HEADER" if header else ""
    delimiter_exp = (
        f"DELIMITER AS '{delimiter}'" if fmt == "CSV" and delimiter else ""
    )
    compression_exp = f"{compress_type.upper()}" if compress_type else ""
    addquotes_exp = "ADDQUOTES" if add_quotes else ""
    escape_exp = "ESCAPE" if escape else ""
    nullas_exp = f"NULL AS '{null_as}'" if null_as else ""
    sql = textwrap.dedent(
        f"""
    UNLOAD ($${statement}$$)
    TO '{unload_temp_path}'
    IAM_ROLE '{ConfigGlobal.redshift_iam_role}'
    {format_exp}
    {header_exp}
    {delimiter_exp}
    {compression_exp}
    {addquotes_exp}
    {nullas_exp}
    {escape_exp}
    ALLOWOVERWRITE
    PARALLEL OFF
    """
    ).replace("\n", " ")
    LOG.debug(f"unload sql: {sql}")
    redshift_query_executor(cluster_id, rs_db, sql, retry=retry, delay=delay)
    unload_result_objs = list_s3_objs(unload_temp_path, None if fmt.lower() == "csv" else fmt.lower())
    assert len(unload_result_objs) == 1, f"Unload query return zero result. SQL: {sql}"
    s3_copy(unload_result_objs[0], s3_uri)
    s3_delete_folder(unload_temp_path)


def redshift_excel_unload_func(
        cluster_id,
        rs_db,
        src_map,
        s3_uri,
        retry=10,
        delay=10):
    """
    redshift unload data as Excel format.
    :param cluster_id: redshift cluster id
    :param rs_db: redshift database name
    :param src_map: {"sheet1": "select xxx", "sheet2": "select 2 xxx"}
    :param s3_uri:  s3://xxxx/test.xlsx
    :param retry: default 10
    :param delay: retry: default 10
    :return: None
    """
    unload_stage = ConfigGlobal.redshift_unload_stage_s3 + str(uuid.uuid4()) + "/"
    csv_sheet_map = dict()
    for sheet_nm, sql in src_map.items():
        tmp_csv_uri = unload_stage + f"{sheet_nm}_tmp.csv"
        csv_sheet_map[tmp_csv_uri] = sheet_nm
        redshift_unload_func(cluster_id, rs_db, sql, tmp_csv_uri, retry=retry, delay=delay)
    assert s3_uri.lower().split(".")[-1] in ("xlsx", "xls", "xlsm"), f"{s3_uri} is not excel file!"
    s3_source_csvs_to_excel(csv_sheet_map, s3_uri)
    s3_delete_folder(unload_stage)


def generate_copy_manifest(load_path_list: list):

    uri_length_map = []
    for s3_uri in load_path_list:
        bucket, prefix, file_nm = s3_parser_to_bucket_prefix(s3_uri)
        s3_response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        obj_list = [(f"s3://{bucket}/{obj.get('Key')}", obj.get("Size")) for obj in s3_response.get("Contents", [])]
        while s3_response.get("IsTruncated"):
            s3_response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix,
                                                    ContinuationToken=s3_response.get("NextContinuationToken"))
            obj_list += [(f"s3://{bucket}/{obj.get('Key')}", obj.get("Size")) for obj in
                         s3_response.get("Contents", [])]
        uri_length_map += obj_list

    manifest_dict = {
        "entries": [{
            "url": s3_uri,
            "mandatory": True,
            "meta": {
                "content_length": content_length
            }
        } for s3_uri, content_length in uri_length_map]
    }

    s3 = boto3.resource('s3', region_name="cn-north-1")
    s3object = s3.Object(s3_parser_to_bucket_prefix(ConfigGlobal.redshift_copy_manifest_s3)[0],
                         s3_parser_to_bucket_prefix(ConfigGlobal.redshift_copy_manifest_s3)[1])
    s3object.put(
        Body=(bytes(json.dumps(manifest_dict).encode('UTF-8')))
    )


def redshift_unload_parallel_func(
        cluster_id,
        rs_db,
        statement,
        s3_path,
        fmt="parquet",
        compress_type: str = None,
        header: bool = False,
        delimiter: str = None,
        add_quotes: bool = False,
        null_as: str = None,
        escape: bool = False,
        retry=10,
        delay=10,
):
    """unload data from redshift to s3.
        * https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html
    :param cluster_id: redshift cluster id
    :param rs_db: redshift database name
    :param statement: query statement to unload data
    :param s3_path: s3 path to store unloaded data
    :param fmt: file format of unloaded data, support CSV | XLSX | PARQUET | JSON
    :param compress_type: compression type of unloaded data, support GZIP | BZIP2 | ZSTD
    :param delimiter: delimiter of unloaded data
    :param header: add header to unloaded data
    :param add_quotes: add quotes to unloaded data
    :param null_as: replace null value with specified value
    :param escape: escape special characters
    :param retry: redshift query retrieve status tims
    :param delay: redshift retrieve interval
    """
    fmt = fmt.upper()
    compress_type = compress_type.upper() if compress_type else None
    assert fmt in [
        "CSV",
        "PARQUET",
        "JSON",
    ], "format must be CSV | XLSX | PARQUET | JSON"
    assert compress_type in [
        "GZIP",
        "BZIP2",
        "ZSTD",
        None,
    ], "compress_type must be GZIP | BZIP2 | ZSTD"
    if fmt in ("PARQUET", "JSON"):
        if header:
            raise ValueError(f"HEADER is not supported for UNLOAD to {fmt}")
        if delimiter:
            raise ValueError(f"DELIMITER is not supported for UNLOAD to {fmt}")
        if add_quotes:
            raise ValueError(f"ADDQUOTES is not supported for UNLOAD to {fmt}")

    format_exp = f"FORMAT AS {fmt}"
    header_exp = "HEADER" if header else ""
    delimiter_exp = (
        f"DELIMITER AS '{delimiter}'" if fmt == "CSV" and delimiter else ""
    )
    compression_exp = f"{compress_type.upper()}" if compress_type else ""
    addquotes_exp = "ADDQUOTES" if add_quotes else ""
    escape_exp = "ESCAPE" if escape else ""
    nullas_exp = f"NULL AS '{null_as}'" if null_as else ""
    sql = textwrap.dedent(
        f"""
    UNLOAD ($${statement}$$)
    TO '{s3_path}'
    IAM_ROLE '{ConfigGlobal.redshift_iam_role}'
    {format_exp}
    {header_exp}
    {delimiter_exp}
    {compression_exp}
    {addquotes_exp}
    {nullas_exp}
    {escape_exp}
    ALLOWOVERWRITE
    PARALLEL ON
    """
    ).replace("\n", " ")
    LOG.debug(f"unload sql: {sql}")
    redshift_query_executor(cluster_id, rs_db, sql, retry=retry, delay=delay)
    unload_result_objs = list_s3_objs(s3_path, None if fmt.lower() == "csv" else fmt.lower())
    LOG.info(f"UNLOAD result files as: {','.join(unload_result_objs)}")

