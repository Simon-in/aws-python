import textwrap
from typing import Optional

from modules.client import _logger
from modules.conf import ConfigGlobal
from modules.redshift import redshift_query_executor
from modules.s3 import (
    s3_delete_folder,
    s3_download,
)
from modules.target_restapi.utils import get_cache_dir, SafeS3Util


LOG = _logger()


def execute_statement(sql, batch=False, **kwargs):
    sql = textwrap.dedent(sql).replace("\n", " ")
    LOG.info(f"Executing Redshift SQL: {sql}")
    return redshift_query_executor(
        cluster_id=ConfigGlobal.redshift_cluster_id,
        db_name=ConfigGlobal.redshift_db_nm,
        sql=sql,
        batch=batch,
        dict_cursor=True,
        kwargs=kwargs,
    )


raw_bucket = f"ph-cdp-raw-{ConfigGlobal.env}-{ConfigGlobal.region}"


def unload_redshift_to_s3(
    query: str,
    s3_path: str,
    output_format="json",
    max_file_size=None,
    parallel: bool = True,
):
    if output_format not in ("json", "parquet"):
        raise ValueError(f"Do not support output format {output_format!r}")

    s3_delete_folder(s3_path)
    exp_parallel = "ON" if parallel else "OFF"
    exp_output_format = output_format.upper()
    exp_extension = output_format.lower()
    sql = textwrap.dedent(
        f"""
        UNLOAD ($${query}$$) 
        TO '{s3_path}' 
        IAM_ROLE '{ConfigGlobal.redshift_iam_role}' 
        FORMAT AS {exp_output_format} 
        ALLOWOVERWRITE 
        NULL AS '' 
        EXTENSION '{exp_extension}' 
        PARALLEL {exp_parallel} 
    """
    )
    if max_file_size:
        sql += f" MAXFILESIZE {max_file_size}"
    execute_statement(sql)


def unload(query, s3_path: str, to_local=False) -> Optional[str]:
    unload_redshift_to_s3(query, s3_path, output_format="json")
    if not to_local:
        return

    cache_dir = get_cache_dir("redshift_unload")
    local_path = SafeS3Util.localize_s3_uri(s3_path, cache_dir, is_file=True)
    LOG.info(f"Downloading data from {s3_path!r} and save to {local_path!r}")
    s3_download(s3_path, local_path)
    return local_path
