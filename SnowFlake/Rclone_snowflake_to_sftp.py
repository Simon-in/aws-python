import datetime
from modules.client import _logger
from bayer_cdp_common_utils.snowflake_handler import SnowflakeConnector
from modules.glue_args import get_glue_args, OptionValue
from modules.glue import get_job_run_id
from modules.conf import ConfigGlobal
from modules.dynamodb import get_entity_config
from modules.s3 import s3_parser_to_bucket_prefix, s3_client, is_s3_path_empty, s3_delete_folder, \
    list_s3_objs
from shlex import quote
from modules.rclone import Config as RConfig
from modules.rclone import RClone

LOG = _logger()


class _RClone(RClone):
    def copy(
        self,
        source,
        destination,
        inplace=False,
        debug=False,
        call_check=False,
        non_blocking=False,
    ):
        params = ["copy", quote(source), quote(destination)]
        if inplace:
            params.insert(1, "--inplace")
        return self.execute(
            *params, debug=debug, call_check=call_check, non_blocking=non_blocking
        )


def copy_rename_s3(source_path, target_path):
    source_bucket, source_prefix, _ = s3_parser_to_bucket_prefix(source_path)
    target_bucket, target_prefix, _ = s3_parser_to_bucket_prefix(target_path)
    copy_source = {'Bucket': source_bucket, 'Key': source_prefix}
    s3_client.copy(copy_source, target_bucket, target_prefix)
    print(f"The file was successfully copied from {source_bucket} to {target_bucket} and renamed to {target_prefix}")


def is_archive_folder(tmp_path):
    current_date = datetime.datetime.now()
    thirty_days_ago = current_date - datetime.timedelta(days=int(is_del_day))
    thirty_days_ago_str = datetime.datetime.strftime(thirty_days_ago, "%Y-%m-%d")
    current_date = datetime.datetime.strftime(current_date, "%Y-%m-%d")
    archive_path = f"s3://bay-cph-cdp-{env}-az-ap-southeast-1/poa/archive/{country_code}/{domain}/{entity}/{current_date}/{load_id}/{file_name}.{suffix}"
    thirty_days_ago_path = f"s3://bay-cph-cdp-{env}-az-ap-southeast-1/poa/archive/{country_code}/{domain}/{entity}/{thirty_days_ago_str}/"
    copy_rename_s3(tmp_path, archive_path)
    s3_sync_sftp(current_date)
    if not is_s3_path_empty(thirty_days_ago_path):
        s3_delete_folder(thirty_days_ago_path)
    print("current_date:", current_date)
    print("thirty_days_ago:", thirty_days_ago_str)


def get_stage_location(snowflake_stage):
    get_stage_location = f"select get_stage_location(@{snowflake_stage})"
    stage_info = sf_conn.execute_query(get_stage_location).fetchone()
    if stage_info:
        stage_name = list(stage_info.keys())[0]
        stage_location = list(stage_info.values())[0]
        print(f"Stage Name: {stage_name}, Stage Location: {stage_location}")
    return stage_location


def s3_sync_sftp(current_date):
    rclone_debug = OptionValue.get_bool(args["RCLONE_DEBUG"])
    sync_inplace = OptionValue.get_bool(args["SYNC_INPLACE"])

    source_alias = entity_config.get("source_alias") or args["SOURCE_ALIAS"]
    source_prefix = f"bay-cph-cdp-{env}-az-ap-southeast-1/poa/archive/{country_code}/{domain}/{entity}/{current_date}/{load_id}/"

    target_alias = entity_config.get("target_alias") or args["TARGET_ALIAS"]
    target_prefix = entity_config.get("target_prefix")

    source_remote = RConfig(alias=source_alias, secret=secret_name).s3()
    target_remote = RConfig(alias=target_alias, secret=secret_name).sftp()

    rclone = _RClone()
    rclone.create_config(source_remote, target_remote)

    source_remote_path = f"{source_alias}:{source_prefix}"
    target_remote_path = f"{target_alias}:{target_prefix}/{country_code}/"

    LOG.info(f"List dir {source_remote_path!r}:")
    rclone.lsd(source_remote_path)

    LOG.info(f"List dir {target_remote_path!r}:")
    rclone.lsd(target_remote_path)

    LOG.info(
        f"Starting rclone copy between {source_remote_path!r} and {target_remote_path!r}"
    )
    ret = rclone.copy(
        source_remote_path,
        target_remote_path,
        inplace=sync_inplace,
        debug=rclone_debug,
        non_blocking=True,
    )
    if ret != 0:
        raise RuntimeError(f"rclone copy failed: {ret}")
    else:
        LOG.info(f"rclone copy successfully")


if __name__ == '__main__':
    args = get_glue_args(
        positional=["JOB_NAME", "DOMAIN", "ENTITY", "LOAD_ID"],
        optional={
            "REGION": ConfigGlobal.region,
            "ENV": ConfigGlobal.env,
            "RCLONE_DEBUG": "true",
            "SYNC_INPLACE": "false",
            "SOURCE_ALIAS": "poa_s3",
            "TARGET_ALIAS": "poa_sftp",
        }
    )
    # 获取glue参数
    domain = args["DOMAIN"]
    entity = args["ENTITY"]
    region = args["REGION"]
    env = args["ENV"]
    job_name = args["JOB_NAME"]
    load_id = args["LOAD_ID"]
    job_run_id = get_job_run_id(job_name)
    glue_start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    LOG.info(f"================{glue_start_time} {job_name} start run ====================")
    entity_config = get_entity_config(domain, entity)
    secret_name = entity_config.get("secret_name", ConfigGlobal.snowflake_secret)
    suffix = entity_config.get('suffix', 'csv')
    delimiter = entity_config.get('delimiter', ',')
    overwrite = entity_config.get('overwrite', True)
    country_code = entity_config.get('country_code')
    sql_query = entity_config.get('sql_query').replace(';', '')
    is_archive = entity_config.get('is_archive', 'True')
    is_del_day = entity_config.get('is_del_day', 30)
    snowflake_stage = entity_config.get('snowflake_stage')
    file_rule = entity_config.get('file_rule')
    now = datetime.datetime.now()
    year = str(now.year)
    month = now.month
    day = str(now.day)
    if month <= 10:
        month = "0" + str(month)
    else :
        month = str(month)
    quarter = "Q" + str((int(month) - 1) // 3 + 1)
    if file_rule == "month":
        file_name = f"{entity}_{month}_{year}"
    elif file_rule == "day":
        file_name = f"{entity}_{day}{month}{year}"
    elif file_rule == "quarter":
        file_name = f"{entity}_{quarter}_{year}"
    field_optionally_enclosed_by = entity_config.get('field_optionally_enclosed_by')
    sf_conn = SnowflakeConnector.from_secret(
        secret_name,
        schema=entity_config['snowflake_staging_schema']
    )
    query = f"""
                COPY INTO @{snowflake_stage}/{country_code}/{domain}/{entity}/{load_id}/{file_name}
                FROM ({sql_query})
                FILE_FORMAT = (TYPE = '{suffix}' FIELD_DELIMITER='{delimiter}'  ENCODING = 'UTF8' COMPRESSION = NONE
                FIELD_OPTIONALLY_ENCLOSED_BY = '{field_optionally_enclosed_by}')
                OVERWRITE = {overwrite}
                HEADER = TRUE;
            """
    sf_conn.execute_query(query)
    if get_stage_location(snowflake_stage):
        stage_location = get_stage_location(snowflake_stage)
        tmp_path = f"{stage_location}{country_code}/{domain}/{entity}/{load_id}/"
    else:
        tmp_path = f"{ConfigGlobal.unload_stage_s3}/{domain}/{entity}/{load_id}/"
    if tmp_path:
        list_path = list_s3_objs(tmp_path)
        for path in list_path:
            if is_archive == 'True':
                is_archive_folder(path)
        if not is_s3_path_empty(tmp_path):
            s3_delete_folder(tmp_path)
    glue_end_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    LOG.info(f"==============={glue_end_time} {job_name} end ==================")