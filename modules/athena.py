"""
Athena 服务操作模块

提供 Athena 数据库的各种操作功能，包括表管理、查询执行、结果处理等
"""
from time import sleep

from botocore.exceptions import ClientError
from modules.client import client, logger
from modules.conf import ConfigGlobal


athena_client = client("athena", max_attempts=10)
LOG = logger()


def sense_athena_table(db_nm, tb_nm, catalog_nm="AwsDataCatalog"):
    try:
        athena_client.get_table_metadata(
            CatalogName=catalog_nm,
            DatabaseName=db_nm,
            TableName=tb_nm
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "MetadataException":
            return False
        else:
            LOG.error(f"Error occurred while get table metadata {db_nm}.{tb_nm}")
            raise e


def get_catalog_columns(db_nm, tb_nm, catalog_nm="AwsDataCatalog"):
    try:
        tb_meta = athena_client.get_table_metadata(
            CatalogName=catalog_nm,
            DatabaseName=db_nm,
            TableName=tb_nm
        ).get("TableMetadata")
        return [col.get("Name") for col in tb_meta.get("Columns")]
    except Exception as e:
        local_logger.error(f"Error occurred while trying to reach columns of catalog table: {db_nm}.{tb_nm} due to {e}")
        raise


def athena_query_executor(statement: str, connections: dict, result_configuration: dict):
    execution_id = athena_client.start_query_execution(
        QueryString=statement,
        QueryExecutionContext=connections,
        ResultConfiguration=result_configuration
    )
    while True:
        resp = athena_client.get_query_execution(QueryExecutionId=execution_id.get("QueryExecutionId"))
        status = resp['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        sleep(0.5)
    if status == 'SUCCEEDED':
        results = athena_client.get_query_results(
            QueryExecutionId=execution_id.get("QueryExecutionId")
        )
        rows = results.get("ResultSet").get("Rows")
        while results.get("NextToken"):
            results = athena_client.get_query_results(
                QueryExecutionId=execution_id.get("QueryExecutionId"),
                NextToken=results.get("NextToken")
            )
            rows += results.get("ResultSet").get("Rows")
        return rows
    else:
        error_msg = resp['QueryExecution']['Status']['AthenaError']["ErrorMessage"]
        raise Exception(f"Athena query failed: {error_msg}")


def athena_result_format(query_result):
    header = [item.get("Data") for item in query_result][0]
    values = [item.get("Data") for item in query_result][1:]
    headers = list(map(lambda x: list(x.values())[0], header))
    records_row = []
    for item in values:
        row = []
        for v in item:
            if v:
                row.append(list(v.values())[0])
            else:
                row.append("")
        records_row.append(row)
    return [dict(zip(headers, record)) for record in records_row]


def get_col_max_length(db_name, table_nm, catalog_nm="AwsDataCatalog"):
    athena_connections = {
        "Database": db_name,
        "Catalog": catalog_nm
    }
    result_config = {
        "OutputLocation": ConfigGlobal.athena_results_s3
    }
    cols = get_catalog_columns(db_nm=db_name, tb_nm=table_nm)
    col_length_dict = {}
    for col in cols:
        get_max_length_sql = f"select max(length({col})) as max_len from {db_name}.{table_nm}"
        athena_res = athena_query_executor(get_max_length_sql, athena_connections, result_config)
        col_length_dict[col] = athena_result_format(athena_res)[0].get("max_len", "16")
    return col_length_dict


def update_partitions(db_name, table_nam, catalog_nm="AwsDataCatalog"):
    repair_cmd = f"msck repair table {db_name}.{table_nam}"
    athena_connections = {
        "Database": db_name,
        "Catalog": catalog_nm
    }
    result_config = {
        "OutputLocation": ConfigGlobal.athena_results_s3
    }
    athena_query_executor(repair_cmd, athena_connections, result_config)
