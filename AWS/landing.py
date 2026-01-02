"""
Common landing job  for bayer-cdp glue etl pipeline: pull data from different source like api, s3, sftp, relationship
database etc...
"""
import datetime
import json
import os
import re
import tempfile

from modules.client import _client, _logger
from modules.conf import ConfigGlobal
from modules.dynamodb import get_entity_config
from modules.glue_args import get_glue_args
from modules.glue import get_job_run_id
from modules.redshift import redshift_insert_func, redshift_query_executor
from modules.s3 import s3_copy_func, s3_client, s3_upload
from modules.secret_manager import get_secret
from modules.dsl import render
from botocore.exceptions import ClientError
from pytz import timezone
from typing import Optional, Dict, List
import requests
from tenacity import *
import msal
import pandas as pd
import time

LOG = _logger()


def get_df_from_rdms(spark, query: str, sys_name: str):
    db_username = conn_info.get('username')
    db_password = conn_info.get('password')
    port = conn_info.get('port')
    server = conn_info.get('server')

    if sys_name == "mssql":
        return spark.read \
            .format("com.microsoft.sqlserver.jdbc.spark") \
            .options(
            driver='com.microsoft.sqlserver.jdbc.SQLServerDriver',
            url=f"jdbc:sqlserver://{server}:{port};databaseName={db_name};",
            query=query,
            user=db_username,
            password=db_password
        ).load()

    elif sys_name == "mysql":
        return spark.read \
            .format("jdbc") \
            .options(
            driver='com.mysql.cj.jdbc.Driver',
            url=f"jdbc:mysql://{server}:{port}/{db_name}",
            query=query,
            user=db_username,
            password=db_password
        ).load()
    elif sys_name == "pgsql":
        return spark.read \
            .format("jdbc") \
            .options(
            driver='org.postgresql.Driver',
            url=f"jdbc:postgresql://{server}:{port}/{db_name}",
            query=query,
            user=db_username,
            password=db_password
        ).load()


def rdms_source_landing_func(spark, landing_bucket):
    # default output format: parquet
    landing_path = f"s3://{landing_bucket}/{domain}/{load_id}/{entity}/"
    load_start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    load_mode = entity_config.get("load_mode", "full")
    inc_filter = ""
    offset_col = entity_config.get('incremental_load_col')
    if load_mode == "incremental":
        assert offset_col, f"Incremental load mode must have 'incremental_load_col' in " \
                           f"configuration: {domain}.{entity}"

        query_max_offset = f"select max(load_offset) from " \
                           f"{ConfigGlobal.redshift_log_schema}.{ConfigGlobal.redshift_incremental_load_log_table}" \
                           f" where src_database = '{db_name}' and src_table = '{entity}' "
        query_max_offset_res = redshift_query_executor(ConfigGlobal.redshift_cluster_id,
                                                       ConfigGlobal.redshift_db_nm,
                                                       query_max_offset)
        max_offset = query_max_offset_res[0][0].get("stringValue")
        if max_offset is not None:
            inc_filter = f"where {offset_col} > {max_offset}"

    elif load_mode == "customized":
        assert entity_config.get('customized_load_sql'), f"Incremental load mode must have 'customized_load_sql' in " \
                                                         f"configuration: {domain}.{entity}"
    query_dict = dict(
        zip(
            ["full", "incremental", "customized"],
            [f"select * from {schema}.{entity}" if schema else f"select * from {entity}",
             f"select * from {schema}.{entity} {inc_filter}" if schema else f"select * from {entity} {inc_filter}",
             entity_config.get("customized_load_sql", '')
             ]
        )
    )
    df = get_df_from_rdms(spark, query=query_dict[load_mode], sys_name=system_nm)
    df = df.select([col(c).cast("string") for c in df.columns])
    df.repartition(int(entity_config.get("landing_load_partitions", 10))).write.parquet(landing_path, mode="overwrite")
    load_end_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    if load_mode == "incremental":
        lz_df = spark.read.parquet(landing_path)
        load_row_cnt = lz_df.count()
        max_record = lz_df.groupby().agg(max_(offset_col)).select(f"max({offset_col})").collect()[0][0]
        load_incremental_catalog_dict = dict(zip(
            ConfigGlobal.rdms_load_catalog_cols, ["Microsoft SQLServer", domain, entity, "AWS S3", landing_path,
                                                  job_name, job_run_id, load_start_time, load_end_time,
                                                  load_row_cnt, max_record]
        ))
        redshift_insert_func(
            ConfigGlobal.redshift_cluster_id,
            ConfigGlobal.redshift_db_nm,
            ConfigGlobal.redshift_log_schema,
            ConfigGlobal.api_load_catalog_cols,
            [load_incremental_catalog_dict]
        )


def s3_source_landing_func(s3_client, landing_bucket):
    suffix = entity_config["landing_file_format"]
    source_bucket = entity_config.get("s3_source_bucket",
                                      f"ph-cdp-nprod-{env}-{region}" if env != "prod" else f"ph-cdp-{env}-{region}")
    source_prefix = render(entity_config.get("s3_source_prefix", f"ph-cdp-sftp-inbound-{env}/{domain}/{entity}/"))
    source_prefix = source_prefix if source_prefix.endswith("/") else source_prefix + "/"

    archive_bucket = entity_config.get("archive_bucket",
                                       f"ph-cdp-nprod-{env}-{region}" if env != "prod" else f"ph-cdp-{env}-{region}")
    archive_prefix = render(entity_config.get("archive_prefix",
                                              f"ph-cdp-sftp-inbound-{env}/archive/{domain}/{entity}/{load_id}/"))
    archive_prefix = archive_prefix if archive_prefix.endswith("/") else archive_prefix + "/"
    LOG.info(f"Source bucket is: {source_bucket} and source Prefix is: {source_prefix}")
    key_items = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
    LOG.info(f"List source keys: {key_items.get('Contents', [])}")
    replace_file_name = render(entity_config.get("replace_file_name", "").replace('{cn_date}', cn_date))
    sr_file_pattern = render(entity_config.get("source_file_pattern", ""))
    data_file_ptn = rf"{source_prefix}{sr_file_pattern}\.{suffix}$"  # use re to filter valid source file
    signal_file_ptn = rf"{source_prefix}{sr_file_pattern}\.ok$"  # use re to filter valid signal file
    LOG.info(f"Data file pattern is: {data_file_ptn} and signal file pattern is: {signal_file_ptn}")
    data_keys = [key.get("Key") for key in key_items.get("Contents", []) if re.match(data_file_ptn, key.get("Key"))]
    signal_keys = [key.get("Key") for key in key_items.get("Contents", []) if re.match(signal_file_ptn, key.get("Key"))]
    is_archive = entity_config.get("is_archive", "true").lower() == "true"

    if replace_file_name:
        assert data_keys.__len__() == 1, "Found multi source file while replace_file_name is needed, " \
                                         "please check configuration"
        for rep_file in replace_file_name.split(";"):
            s3_copy_func(source_bucket,
                         data_keys[0],
                         source_bucket,
                         source_prefix + rep_file + "." + suffix)
    for data_key in data_keys:
        file_nm = data_key.split("/")[-1]
        target_key = f"{domain}/{load_id}/{entity}/{file_nm}"
        s3_copy_func(source_bucket, data_key, landing_bucket, target_key)  # copy source file to landing layer
        if is_archive:
            s3_copy_func(source_bucket, data_key, archive_bucket, archive_prefix + file_nm)  # archive data

    if is_archive:
        s3_client.delete_objects(  # delete files in source prefix
            Bucket=source_bucket,
            Delete={
                "Objects": [{"Key": key} for key in [*data_keys, *signal_keys]]
            }
        )


class DataverseError(Exception):
    pass


class DataverseAuthError(DataverseError):
    pass


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=60),
    retry=retry_if_exception_type(DataverseAuthError)
)
def acquire_dataverse_token(config, token):
    scopes = [f"https://{config.get('org')}.crm.dynamics.cn/.default"]
    app = msal.ConfidentialClientApplication(
        client_id=config.get("client_id"),
        client_credential=config.get("client_secret"),
        authority=f"https://login.microsoftonline.com/{config.get('tenant_id')}",
        token_cache=msal.TokenCache()
    )
    account = app.get_accounts()
    result = app.acquire_token_silent(
        scopes=scopes,
        account=account,
        force_refresh=False
    )
    if not result:
        if not token.get('refresh_token'):
            raise ValueError("No refresh token available")
        result = app.acquire_token_by_refresh_token(token.get('refresh_token'), scopes=scopes)
    if "access_token" not in result:
        error_msg = result.get("error_description", "Unknown authentication error")
        raise DataverseAuthError(f"Authentication failed: {error_msg}")

    return result["access_token"], result["refresh_token"]


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=60),
    retry=retry_if_exception_type(requests.HTTPError)
)
def make_dataverse_request(
        url: str,
        token: str,
        params: Optional[Dict] = None,
        page_size: int = 5000
):
    headers = {
        "Authorization": f"Bearer {token}",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Accept": "application/json",
        "Prefer": f"odata.maxpagesize={page_size}"
    }
    response = requests.get(
        url=url,
        headers=headers,
        params=params,
        timeout=50
    )
    response.raise_for_status()
    return response


def dataverse_source_landing_func(
        table: str,
        entity_config: dict,
        page_size: int = 5000
):
    # default landing file format parquet
    sm_client = _client("secretsmanager")
    try:
        secret_name = entity_config.get("conn_id")
        token_secret_name = entity_config.get("token_id")
        select_col = entity_config.get("select_col")
        filter_col = entity_config.get("filter", None)
        top = entity_config.get("top")
        filter_p1 = entity_config.get("p1")
        filter_p2 = entity_config.get("p2")
        config = get_secret(secret_name)[1]
        token = get_secret(token_secret_name)[1]
        base_url = f"https://{config.get('org')}.crm.dynamics.cn/api/data/v9.2/{table}"
        params = {}
        if select_col:
            params["$select"] = select_col
        if filter_col and filter_p1 and filter_p2:
            params["$filter"] = filter_col
            params["@p1"] = filter_p1
            params["@p2"] = filter_p2
        if top:
            params["$top"] = top
        all_data = []
        next_url = base_url
        tmp_dir = tempfile.mkdtemp()
        total_records = 0
        LOG.info(f"Starting to fetch {table} data with page size: {page_size}")
        access_token, refresh_token = acquire_dataverse_token(config, token)
        while next_url:
            response = make_dataverse_request(next_url, access_token, params, page_size)
            if response.status_code == 401:
                LOG.warning("Access token expired, refreshing...")
                access_token, refresh_token = acquire_dataverse_token(config, token)
                response = make_dataverse_request(next_url, access_token, params, page_size)
            response_data = response.json()
            all_data.extend(response_data.get("value", []))
            total_records += len(all_data)
            next_url = response_data.get("@odata.nextLink", None)
            if next_url:
                params = {}
            LOG.info(f"Retrieved {len(all_data)} records, next page available: {next_url}")
            if len(all_data) == 0:
                LOG.info(f"Retrieved {len(all_data)} {table} records. Task terminated.")
            elif len(all_data) >= 30000 or (next_url is None and len(all_data) > 0):
                timestamp = datetime.datetime.now(tz=timezone("Asia/Shanghai")).strftime("%Y%m%d%H%M%S")
                tmp_file = f"{timestamp}_{hash(str(all_data[:1]))}.parquet"
                tmp_path = os.path.join(tmp_dir, tmp_file)
                df = pd.DataFrame(all_data)
                df_str = df.astype('string')
                df_str.to_parquet(tmp_path, engine="pyarrow")
                LOG.info(f"Temporary Parquet file persisted: {tmp_path}")
                dst_path = f"s3://{landing_bucket}/{domain}/{load_id}/{table}/{tmp_file}"
                s3_upload(src_path=tmp_path, dst_path=dst_path)
                LOG.info(f"Temporary file uploaded to S3: {dst_path}")
                os.remove(tmp_path)
                all_data = []
        try:
            if refresh_token:
                sm_client.update_secret(
                    SecretId=token_secret_name,
                    SecretString=json.dumps(
                        {
                            "refresh_token": refresh_token
                        }
                    )
                )
                LOG.info("Refresh token updated successfully")
                if os.path.exists(tmp_dir):
                    for file in os.listdir(tmp_dir):
                        os.remove(os.path.join(tmp_dir, file))
                    os.rmdir(tmp_dir)
                LOG.info("Temporary directory cleaned up.")
        except ClientError as e:
            LOG.error(f"Failed to update refresh token: {str(e)}")
        finally:
            LOG.info(f"Successfully retrieved {total_records} {table} records")
    except Exception as e:
        LOG.error(f"Data acquisition failure | Exception: {type(e).__name__} | Details: {str(e)}")
        raise


def StockForecast_source_landing_func(entity, entity_config):
    # default landing file format parquet
    secret_name = entity_config.get("conn_id", "phcdp/stockforecast")
    url = entity_config.get("api_url")
    max_page_size = entity_config.get("pagesize", 5000)
    request_delay = entity_config.get("request_delay", 1)

    stock_config = get_secret(secret_name)[1]
    stockToken = stock_config.get("stockToken")
    headers = {
        'Content-Type': 'application/json',
        'stockToken': stockToken
    }
    year_month_list = get_target_months()
    tmp_dir = tempfile.mkdtemp()
    version_list = entity_config.get("version").split(",")
    try:
        all_data = []
        for year_month in year_month_list:
            year = year_month[:4]
            month = year_month[4:]

            for version in version_list:
                LOG.info(f"Starting sync for target months: {year}-{month}-{version}")

                total_count = 0
                current_page = 1

                while True:
                    payload = json.dumps({
                        "currentPage": current_page,
                        "pageSize": max_page_size,
                        "year": year,
                        "month": month,
                        "version": version
                    })

                    response = requests.post(url, headers=headers, data=payload, timeout=300)

                    if response.status_code != 200:
                        raise Exception(f"API request failed: {response.status_code}, {response.text}")

                    data = response.json()
                    items = data.get("data", {}).get("list", [])
                    is_next_page = data.get("data", {}).get("hasNextPage", False)

                    if not items:
                        LOG.info(f"No data available for {year}-{month}-{version}, stopping pagination.")
                        break

                    all_data.extend(items)
                    current_page_count = len(items)
                    total_count += current_page_count
                    LOG.info(f"Page {current_page} for {year}-{month}-{version}: {current_page_count} items (Total: {total_count})")

                    if not is_next_page:
                        LOG.info(f"No more pages for {year}-{month}-{version}.")
                        break

                    current_page += 1
                    time.sleep(request_delay)
        if all_data:
            _process_batch_data(all_data, tmp_dir, entity, landing_bucket, domain, load_id)

        LOG.info("Data synchronization completed")

    except Exception as e:
        LOG.error(f"Data acquisition process failed: {str(e)}")
        raise
    finally:
        _cleanup_temp_dir(tmp_dir)


def _process_batch_data(data_list, tmp_dir, entity, landing_bucket, domain, load_id):
    if not data_list:
        return

    try:
        timestamp = datetime.datetime.now(tz=timezone("Asia/Shanghai")).strftime("%Y%m%d%H%M%S")
        tmp_file = f"{timestamp}_{hash(str(data_list[:1]))}.parquet"
        tmp_path = os.path.join(tmp_dir, tmp_file)
        dst_path = f"s3://{landing_bucket}/{domain}/{load_id}/{entity}/{tmp_file}"
        df = pd.DataFrame(data_list)
        df = df.replace(['', ""], pd.NA).convert_dtypes()
        df_str = df.astype('string')
        df_str.to_parquet(tmp_path, engine="pyarrow", index=False)
        LOG.info(f"Temporary Parquet file saved: {tmp_path}")
        s3_upload(src_path=tmp_path, dst_path=dst_path)
        LOG.info(f"Temporary file uploaded to S3: {dst_path}")
        os.remove(tmp_path)

    except Exception as e:
        LOG.error(f"Batch data processing failed: {str(e)}")
        raise


def _cleanup_temp_dir(tmp_dir):
    try:
        if os.path.exists(tmp_dir):
            for file in os.listdir(tmp_dir):
                file_path = os.path.join(tmp_dir, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            os.rmdir(tmp_dir)
            LOG.info("Temporary directory cleanup completed")
    except Exception as e:
        LOG.warning(f"Temporary directory cleanup failed: {str(e)}")


def get_target_months():
    from dateutil.relativedelta import relativedelta
    current_date = datetime.datetime.now()
    previous_month = (current_date - relativedelta(months=1)).strftime("%Y%m")
    current_month = current_date.strftime("%Y%m")
    next_month = (current_date + relativedelta(months=1)).strftime("%Y%m")
    year_month_list = [previous_month, current_month, next_month]
    return year_month_list


if __name__ == '__main__':
    args = get_glue_args(
        positional=[
            "JOB_NAME",
            "DOMAIN",
            "ENTITY",
            "LOAD_ID"
        ],
        optional={
            "REGION": ConfigGlobal.region,
            "ENV": ConfigGlobal.env
        },
    )
    job_name = args['JOB_NAME']
    job_run_id = get_job_run_id(job_name, args)
    domain = args['DOMAIN']
    entity = args['ENTITY']
    env = args['ENV']
    region = args['REGION']
    load_id = args["LOAD_ID"]

    cn_date = datetime.datetime.now(tz=timezone("Asia/Shanghai")).strftime("%Y%m%d")
    entity_config = get_entity_config(domain, entity)
    source_system = entity_config['source_system']
    landing_bucket = f"ph-cdp-landing-{env}-{region}"
    if len(source_system.split("_")) > 0:
        system_nm = source_system.split("_")[0]
    else:
        system_nm = source_system

    if system_nm == "sftp":
        s3_source_landing_func(_client("s3"), f"ph-cdp-landing-{env}-{region}")
    elif system_nm in ("mssql", "mysql", "pgsql"):
        from pyspark.sql.functions import max as max_, col
        from pyspark.sql import SparkSession

        spark_session = SparkSession.builder \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

        db_name = entity_config.get("src_database", "dev")
        schema = entity_config.get("src_schema", "public")
        conn_info = get_secret(entity_config.get("conn_id", f"phcdp/{system_nm}/{db_name}"))[1]

        rdms_source_landing_func(spark_session, f"ph-cdp-landing-{env}-{region}")

    elif system_nm == "salesforce":
        sf_identifier = entity_config["salesforce_identifier"]
        sf_name = entity_config["salesforce_name"]
        sql_query = entity_config.get("sql_query")
        if sql_query:
            salesforce_source_landing_func(
                sql_query, f"ph-cdp-landing-{env}-{region}"
            )
        else:
            raise ValueError(f"sql_query can not be empty!")

    elif system_nm == "dataverse":
        dataverse_source_landing_func(entity, entity_config)

    elif system_nm == "stockforecast":
        StockForecast_source_landing_func(entity, entity_config)

    else:
        raise SystemError(f"Unsupported system found: {system_nm}")