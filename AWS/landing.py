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

    entity_config = get_entity_config(domain, entity)
    source_system = entity_config['source_system']
    landing_bucket = f"landing-{env}-{region}"
    if len(source_system.split("_")) > 0:
        system_nm = source_system.split("_")[0]
    else:
        system_nm = source_system

    if system_nm == "dataverse":
        dataverse_source_landing_func(entity, entity_config)

    elif system_nm == "stockforecast":
        StockForecast_source_landing_func(entity, entity_config)

    else:
        raise SystemError(f"Unsupported system found: {system_nm}")