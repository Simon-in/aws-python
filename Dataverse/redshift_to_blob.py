from modules.client import logger
from modules.conf import ConfigGlobal
from modules.glue import get_glue_args
from modules.redshift import redshift_query_executor
from modules.s3 import s3_download, check_if_exist_obj
from modules.secret_manager import get_secret
import tempfile
import textwrap
import os
import uuid
from pathlib import Path
from azure.core.credentials import AzureNamedKeyCredential
from azure.storage.blob import BlobServiceClient
from tenacity import *

LOG = logger()


class AzureAuthError(Exception):
    pass


class azure_blob:
    def __init__(self):
        _, secrets = get_secret(args["SECRET"])
        self.access_key = secrets.get("key")
        self.endpoint = secrets.get("endpoint")
        self.account_name = secrets.get("account")
        self.table = args["TABLE"]
        self.blob = f"sync/{self.table}/"
        self.statement = args["SQL"]
        self.s3 = args["S3_PATH"]
        self.container_name = args["CONTAINER"]
        self.region = args["REGION"]
        self.env = args["ENV"]
        self.cluster_id = args["REDSHIFT_CLUSTER"]
        self.rs_db = args["RS_DB"]
        self.load_id = args["LOAD_ID"]

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        retry=retry_if_exception_type(AzureAuthError)
    )
    def AzureCredential(self):
        try:
            credential = AzureNamedKeyCredential(self.account_name, self.access_key)
            blob_service_client = BlobServiceClient(account_url=self.endpoint, credential=credential)
            container_client = blob_service_client.get_container_client(container=self.container_name)
            LOG.info(
                "Azure Blob Storage authentication successful. account: %s, container: %s, auth_method: AzureNamedKeyCredential",
                self.account_name,
                self.container_name
            )
            return container_client
        except AzureAuthError as e:
            LOG.error(
                "Azure Blob Storage authentication failed. account: %s, container: %s, auth_method: AzureNamedKeyCredential",
                self.account_name,
                self.container_name
            )
            raise

    def azure_blob_upload(self):
        try:
            container_client = self.AzureCredential()
            parquet_files = self.redshift_upload_local()
            if parquet_files is None:
                LOG.info("Data unload process completed successfully. No data found to export.")
                return
            else:
                self.azure_blob_delete(container_client)
                for p in parquet_files:
                    name = os.path.basename(p)
                    blob_name = f"{self.blob}{name}"
                    LOG.info(f"Uploading local file {name} to Azure Blob: {blob_name}")
                    with open(p, mode="rb") as data:
                        blob_client = container_client.upload_blob(
                            name=blob_name,
                            data=data,
                            overwrite=True
                        )
                    LOG.info(f"Successfully uploaded to Azure Blob Storage: {blob_name}")
        except Exception as e:
            LOG.error(f"Azure Blob Upload Failed : {str(e)}")
            raise

    def azure_blob_delete(self, container_client):
        try:
            LOG.info(f"Starting deletion process for folder: {self.blob}")
            blob_p = self.blob + self.table
            blobs_to_delete = list(container_client.list_blobs(name_starts_with=blob_p))
            LOG.info(f"Number of blobs found for deletion: {len(blobs_to_delete)}")
            for blob in blobs_to_delete:
                LOG.info(f"Starting deletion process for file: {blob.name}")
                blob_client = container_client.get_blob_client(blob.name)
                blob_client.delete_blob(delete_snapshots="include")
                LOG.info(f"File deleted successfully. blob_name: {blob.name}")
        except Exception as e:
            LOG.error(f"Error during deletion: {str(e)}")
            raise

    def redshift_upload_local(self):
        try:
            Bucket = f"ph-{'prod' if self.env == 'prod' else f'nprod-{self.env}'}-cn-north-1"
            prefix = f"{self.s3}{self.load_id}/{self.table}/"
            sql = textwrap.dedent(
                f"""
                    UNLOAD ($${self.statement}$$)
                    TO 's3://{Bucket}/{prefix}{self.table}'
                    IAM_ROLE '{ConfigGlobal.redshift_iam_role}'
                    FORMAT AS PARQUET;
                """
            ).replace("\n", " ")
            redshift_query_executor(self.cluster_id, self.rs_db, sql, retry=10, dylay=10)
            if not check_if_exist_obj(Bucket, prefix):
                return None
            else:
                tmp_dir = tempfile.mkdtemp()
                file = f"{str(uuid.uuid4())}.parquet"
                tmp_path = os.path.join(tmp_dir, file)
                s3_download(f"s3://{Bucket}/{prefix}", tmp_path)
                LOG.info(f"Successfully download file to {tmp_path}")
                folder = Path(tmp_dir)
                parquet_files = [f for f in folder.rglob("*.parquet") if f.is_file()]
                return parquet_files
        except Exception as e:
            LOG.error(f"Redshift unload Failed : {str(e)}")
            raise


if __name__ == "__main__":
    """
            SQL : select * from model_retail.v_test_lin_export
            SECRET : redshift/dataverse
            S3_PATH : ph-sftp-outbound-dev/retail/
            CONTAINER : data-management-dev
            TABLE : test_lin
            RS_DB : retail_dev
    """
    args = get_glue_args(
        positional=["LOAD_ID", "SQL", "S3_PATH", "SECRET", "CONTAINER", "RS_DB", "TABLE"],
        optional={
            "REGION": ConfigGlobal.region,
            "ENV": ConfigGlobal.env,
            "REDSHIFT_CLUSTER": ConfigGlobal.redshift_cluster_id,
        },
    )
    azure_blob().azure_blob_upload()
