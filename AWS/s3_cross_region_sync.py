from modules.conf import ConfigGlobal
from modules.glue_args import get_glue_args
from modules.client import logger
from modules.redshift import redshift_query_executor
import subprocess
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

LOG = _logger()


class BatchProcessingError(Exception):
    pass


def check_env():
    try:
        subprocess.run(["aws", "--version"], capture_output=True, check=True)
    except Exception as e:
        raise LOG.error(
            "The current environment does not have the AWS CLI — please confirm that the environment is correct.")


def get_data(cluster_id, rs_db, sql) -> List:
    try:
        s3_path_list = []
        records = redshift_query_executor(cluster_id, rs_db, sql)
        if not records:
            raise SystemError(f"SQL not query data : {sql}")
        for info in records:
            for i in range(len(info)):
                s3_path_list.append(info[i].get('stringValue'))
        return s3_path_list
    except Exception as e:
        raise SystemError(
            f'Database query failed — please confirm that the SQL statement is correct. SQL --- {sql}')


def sync(source, dest, source_region, dest_region, exclude):
    cmd = ["aws", "s3", "sync", source, dest]
    if source_region:
        cmd.extend(["--source-region", source_region])
    if dest_region:
        cmd.extend(["--region", dest_region])
    if exclude:
        exclude_list = []
        for i in exclude.split(","):
            exclude_list.append(i)
        for p in exclude_list:
            cmd.extend(["--exclude", p])
    _print_info(source)
    LOG.info(f"Sync: {source} → {dest}")
    LOG.info(f"Command: {' '.join(cmd)}\n")
    run(cmd)


def cp(src_bucket, source, dest, load_id, exclude):
    src = "s3://" + src_bucket + '/' + source
    dst = "s3://" + dest + load_id + '/' + source
    cmd = ["aws", "s3", "cp", src, dst]
    if exclude:
        exclude_list = []
        for i in exclude.split(","):
            exclude_list.append(i)
        for p in exclude_list:
            cmd.extend(["--exclude", p])
    LOG.info(f"Sync: {src} → {dst}")
    LOG.info(f"Command: {' '.join(cmd)}\n")
    run(cmd)


def _print_info(source):
    try:
        result = subprocess.run(
            ["aws", "s3", "ls", source, "--recursive", "--summarize"],
            capture_output=True, text=True, check=True
        )
        ls_info = result.stdout
        LOG.info(f"\nSource: {source}")
        LOG.info(f"\nFile : {ls_info}")
    except Exception as e:
        LOG.error(f"\nSource: {source} (info unavailable: {e})\n")


def run(cmd):
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        raise SystemError(f"Sync failed: {e}")


def _batch(file_list, src_bucket, dest, load_id, exclude):
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for source in file_list:
            if not source:
                LOG.warning("SKIP")
                continue
            future = executor.submit(
                cp,
                src_bucket,
                source,
                dest,
                load_id,
                exclude
            )
            futures.append(future)
        errors = []
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                LOG.error(f"File {source} failed: {str(e)}")
                errors.append(source)
        if errors:
            LOG.error(f"Batch failed for {len(errors)} files: {errors}")
            raise BatchProcessingError(f"Failed files: {errors}")


if __name__ == "__main__":
    args = get_glue_args(
        positional=[
            "JOB_NAME",
            "LOAD_ID",
            "SYNC_TYPE"
        ],
        optional={
            "REDSHIFT_CLUSTER": ConfigGlobal.redshift_cluster_id,
            "RS_DB": ConfigGlobal.redshift_db_nm,
            "REGION": ConfigGlobal.region,
            "ENV": ConfigGlobal.env,
            "SOURCE_REGION": "",
            "DEST_REGION": "",
            "EXCLUDE": "",
            "SQL": "",
            "SOURCE": "",
            "SOURCE_BUCKET": "",
            "DEST": "",
        },
    )
    sync_type = args["SYNC_TYPE"]
    source_region = args["SOURCE_REGION"] or None
    dest_region = args["DEST_REGION"] or None
    exclude = args["EXCLUDE"] or None
    dest = args["DEST"]
    load_id = args["LOAD_ID"]

    check_env()

    if sync_type == 's3':
        source = "s3://" + args["SOURCE"]
        dest = "s3://" + dest + load_id + '/'
        sync(source, dest, source_region, dest_region, exclude)
    elif sync_type == 'table':
        cluster_id = args["REDSHIFT_CLUSTER"]
        rs_db = args["RS_DB"]
        sql = args["SQL"] or None
        src_bucket = args["SOURCE_BUCKET"] or None
        s3_list = get_data(cluster_id, rs_db, sql)
        _batch(s3_list, src_bucket, dest, load_id, exclude)
    else:
        raise SystemError(f"Unsupported sync_type found: {sync_type}")


