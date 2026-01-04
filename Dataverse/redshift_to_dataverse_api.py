from modules.client import logger
from modules.conf import ConfigGlobal
from modules.dynamodb import get_entity_config
from modules.glue_args import get_glue_args
from modules.secret_manager import get_secret
from modules.target_restapi.source_redshift import (
    unload as redshift_unload,
)
from modules.target_restapi.utils import chunked_read_file
from modules.target_restapi.wal import S3WAL
import json
import threading
import msal
import requests
from pathlib import Path
from typing import Dict, List, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import *


LOG = logger()


class DataverseError(Exception):
    pass


class DataverseAuthError(DataverseError):
    pass


class RetailPPSuccessWAL(S3WAL):
    def __init__(self, domain, entity, load_id, bucket: str = None):
        if not bucket:
            bucket = f"ph-raw-{env}-cn-north-1"
        self.filename = (
            f"s3://{bucket}/{domain}/wal_log/{entity}/{load_id}/success/{load_id}.json"
        )
        super(RetailPPSuccessWAL, self).__init__(self.filename)


class RetailPPFailedWAL(S3WAL):
    def __init__(self, domain, entity, load_id, bucket: str = None):
        if not bucket:
            bucket = f"ph-raw-{env}-cn-north-1"
        self.filename = f"s3://{bucket}/{domain}/wal_log/{entity}/{load_id}/failed/{load_id}.json"
        super(RetailPPFailedWAL, self).__init__(self.filename)


class ThreadSafeWAL:
    def __init__(self, wal):
        self.wal = wal
        self.lock = threading.Lock()

    def append(self, record):
        with self.lock:
            self.wal.append(record)


class RetailPPAPI:
    def __init__(
            self,
            config: dict,
            token_config: dict
    ):
        self._client = msal.ConfidentialClientApplication(
            client_id=config.get("client_id"),
            client_credential=config.get("client_secret"),
            authority=f"https://login.microsoftonline.com/{config.get('tenant_id')}",
            token_cache=msal.TokenCache()
        )
        self.token_config = token_config
        self.scopes = f"https://{config.get('org')}.crm.dynamics.cn/"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.get_token()}",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8"
        })


    @property
    def client(self):
        return self._client

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        retry=retry_if_exception_type(DataverseAuthError)
    )
    def get_token(self) -> str:
        app = self.client
        result = app.acquire_token_by_refresh_token(self.token_config.get('refresh_token'), scopes=[f"{self.scopes}.default"])
        if "access_token" not in result:
            error_msg = result.get("error_description", "Unknown authentication error")
            raise DataverseAuthError(f"Authentication failed: {error_msg}")
        return result["access_token"]

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        retry=retry_if_exception_type(requests.HTTPError)
    )
    def bulk_write(
            self,
            table_name,
            records: List[Dict],
            request_fields: List[str],
            pre_hook: Callable = None
    ):
        url = f"{self.scopes}api/data/v9.2/{table_name}/Microsoft.Dynamics.CRM.CreateMultiple"
        if not records:
            LOG.info(f"No records to write")
            return
        data = []
        for record in records:
            item = create_post_data(record, request_fields)
            if callable(pre_hook):
                item = pre_hook(item)
            data.append(item)
        payload = {"Targets": data}
        response = self.session.post(url, json=payload)
        if response.status_code == 401:
            self.session.headers.update({
                "Authorization": f"Bearer {self.get_token()}",
                "OData-MaxVersion": "4.0",
                "OData-Version": "4.0",
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8"
            })
            response = self.session.post(url, json=payload)
        response.raise_for_status()
        return response


def threaded_bulk_write(
        api,
        data_file: str,
        max_workers: int = 3,
        batch_size: int = 1000
):

    with RetailPPSuccessWAL(
            domain=domain, entity=entity, load_id=load_id
    ) as success_wal, RetailPPFailedWAL(
        domain=domain, entity=entity, load_id=load_id
    ) as failed_wal, open(
        data_file, encoding="utf-8"
    ) as fo:

        safe_success = ThreadSafeWAL(success_wal)
        safe_failed = ThreadSafeWAL(failed_wal)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for records in chunked_read_file(batch_size, fo):
                records = [json.loads(record.strip()) for record in records]
                raw_records = json.dumps(records, ensure_ascii=False)
                future = executor.submit(
                    process_batch,
                    api, entity, records, raw_records,
                    safe_success, safe_failed
                )
                futures.append(future)
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    LOG.error(f"Data batch processing error: {e}")
                    raise  # Re-raise the exception to propagate error


def process_batch(api, entity, records, raw_records, success_wal, failed_wal):
    try:
        api.bulk_write(
            entity, records, api_request_fields, pre_hook=api_pre_hook
        )
        LOG.info(f"Successfully wrote {len(records)} records to Dataverse")
        success_wal.append(raw_records)
    except Exception as e:
        LOG.error(f"Write failed: {e}\n Record: {raw_records[:200]}...")
        failed_wal.append(raw_records)
        raise


def parse_request_fields(api_request_fields):
    if not api_request_fields or not isinstance(api_request_fields, str):
        raise ValueError("Invalid api_request_fields: must be a non-empty string")
    return [field.strip() for field in api_request_fields.split(',') if field.strip()]


def create_post_data(record: Dict, request_fields: List[str]) -> Dict:
    data = {}
    for field in request_fields:
        data[f"{field_title}_{field}"] = record[field]
    return data


if __name__ == "__main__":
    args = get_glue_args(
        positional=["LOAD_ID", "DOMAIN", "ENTITY"],
        optional={
            "REGION": ConfigGlobal.region,
            "ENV": ConfigGlobal.env,
        },
    )
    domain = args["DOMAIN"]
    entity = args["ENTITY"]
    load_id = args["LOAD_ID"]
    region = args["REGION"]
    env = args["ENV"]
    """  Dynamodb config
        {
         "domain": "enriched_retail",
         "entity": "sales_data_100k_xinyus",
         "api_conn_id": "redshift/retail",
         "api_conn_token": "redshift/retailtoken",
         "api_request_fields": "orderid,customer,product,amount,date",
         "extract_query": "select * from enriched_retail.sales_data",
         "field_title": "crc5f"
        }
    """
    if entity[-2:] == "es":
        entity_type = entity[:-2]
    else:
        entity_type = entity[:-1]

    def api_pre_hook(record: Dict):
        record["@odata.type"] = f"Microsoft.Dynamics.CRM.{entity_type}"
        return record

    entity_config = get_entity_config(domain, entity)
    extract_query = entity_config["extract_query"]
    api_request_fields = parse_request_fields(entity_config["api_request_fields"])
    api_conn_id = entity_config["api_conn_id"]
    field_title = entity_config["field_title"]
    api_conn_token = entity_config["api_conn_token"]
    _, api_config = get_secret(api_conn_id)
    _, token_config = get_secret(api_conn_token)
    api = RetailPPAPI(api_config, token_config)

    LOG.info(f"Login success, token: {api.get_token()}")
    local_path = redshift_unload(
        extract_query,
        s3_path=f"s3://ph-raw-{env}-cn-north-1/{domain}/wal_log/{entity}/{load_id}/source/",
        to_local=True,
    )
    jsonl_files = list(Path(local_path).glob("*.json"))
    for data_file in jsonl_files:
        LOG.info(f"Processing {data_file}")
        threaded_bulk_write(api, data_file)
