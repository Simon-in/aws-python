"""
common python functions for bayer cdp glue jobs
"""
import sys
import time
from typing import Dict

from awsglue.utils import getResolvedOptions
from modules.client import client, logger
from modules.s3 import s3_source_excel_transfer
from modules.awsglue_patch import is_glue_spark_script
if is_glue_spark_script():
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import explode, to_json

glue_client = client("glue")
local_logger = logger()
LOG = local_logger


def get_job_run_id_within_context():

    if is_glue_spark_script():
        args = getResolvedOptions(sys.argv, [])
        job_run_id = args.get("JOB_RUN_ID")
        return job_run_id
    return ""


def identical_job_run_arguments(post: Dict, response: Dict) -> bool:
    """check whether arguments are identical.

    :param post: arguments job posted
    :param response: arguments job run returned

    """
    for arg_name in response:
        if post[arg_name.replace("--", "")] != response[arg_name]:
            return False
    return True


def get_job_run_id(job_name, job_arguments: Dict=None, retry=3, delay=20) -> str:
    """
    get current running glue job id with glue job name

    :param job_name: glue job name
    :param job_arguments: Arguments for the job run created
    :param retry: times to retry
    :param delay: seconds to wait before retry
    :return: glue job id
    """
    job_run_id = get_job_run_id_within_context()
    if job_run_id:
        LOG.debug(f"Got job_run_id from spark script arguments")
        return job_run_id

    response = glue_client.get_job_runs(JobName=job_name)
    while retry != 0:
        runs = response["JobRuns"]
        running_runs = [run for run in runs if run["JobRunState"] == "RUNNING"]
        if running_runs:
            if job_arguments:
                runs_with_args = [run for run in running_runs if run.get("Arguments") is not None]
                if not runs_with_args:
                    LOG.warning(f"No job runs with `Arguments` found, "
                                f"`Arguments` is only available in the JobRun triggered by `glue_client.start_job_run`.")
                else:
                    for run in runs_with_args:
                        if identical_job_run_arguments(job_arguments, run["Arguments"]):
                            job_run_id = run["Id"]
                            LOG.debug(f"Got job_run_id use job_name and job_arguments")
                            return job_run_id

            job_run_id = running_runs[0]['Id']
            if len(running_runs) > 1:
                LOG.warning(f"More than one running job found, return the first one")
            LOG.debug(f"Got job_run_id use job_name")
            return job_run_id
        else:
            if retry == 1:
                local_logger.error("Have tried 3 times to get job_id but failed")
                raise Exception("Job start failed!")
            else:
                retry -= 1
                local_logger.info("waiting for job run")
                time.sleep(delay)
    return job_run_id


def get_df_from_s3(spark, file_format, source_path, **kwargs):
    """
    using spark_read to get dataframe object from source file
    :param spark:  sparkSession object
    :param file_format: source file format eg: csv xlsx parquet...
    :param source_path: source file s3 path
    :param kwargs: configurations params like header, row_tag..
    :return: dataframe
    """
    # read data from lz
    is_header = False if not kwargs.get("is_header") else True if kwargs.get("is_header").upper() == 'TRUE' else False
    sheet_name = 0 if kwargs.get("sheet_name") is None else kwargs.get("sheet_name")
    row_tag = kwargs.get("row_tag")
    skip_row = kwargs.get("skip_row")
    use_cols = kwargs.get("use_cols")
    excel_dtypes = kwargs.get("excel_dtypes")
    delimiter = "," if kwargs.get("delimiter") is None else \
        kwargs.get("delimiter").encode("utf-8").decode("unicode_escape")
    escape = "\\" if kwargs.get("escape") is None else \
        kwargs.get("escape").encode("utf-8").decode("unicode_escape")
    file_format = file_format.lower()

    lz_df: DataFrame
    if file_format == "tsv":
        file_format = "csv"
    if file_format in ['csv', 'txt']:
        lz_df = spark.read.csv(source_path, header=is_header, sep=delimiter, escape=escape)

    elif file_format == 'parquet':
        lz_df = spark.read.parquet(source_path)

    elif file_format == 'xml':
        lz_df = spark.read.format('com.databricks.spark.xml').options(rowTag=row_tag).load(source_path)

    elif file_format == 'json':
        lz_df = spark.read.json(source_path)
        dtypes_tp = lz_df.dtypes[0]
        if dtypes_tp[1][:5] == 'array':
            lz_df = lz_df.withColumn("nest", explode(dtypes_tp[0])).select("nest.*")
        elif dtypes_tp[1] == "string":
            for col in lz_df.dtypes:
                col_name = col[0]
                if col[1][:5] in ('struc', 'array'):
                    lz_df = lz_df.withColumn(col_name, to_json(col_name))
        else:
            raise Exception("Currently not support such structure")

    elif file_format.lower() == 'xlsx' or 'xlsm' or 'xls' or 'xlsb':
        s3_source_excel_transfer(lz_folder=source_path,
                                 sheet_name=sheet_name,
                                 ext=file_format,
                                 skip_row=skip_row,
                                 use_cols=use_cols,
                                 excel_dtypes=excel_dtypes
                                 )
        source_path = source_path if source_path.endswith("/") else source_path + "/"
        lz_df = spark.read.csv(source_path + "sheet/", header=is_header,  sep="\1", quote="\2", lineSep="\3")

    else:
        local_logger.error(f"Invalid source file format found: {file_format}")
        raise Exception("Undefined file format")

    return lz_df


def fetch_row_count_by_id(df, job_log_json_list):
    """
    get row count from result dataframe
    :param df: spark dataframe object
    :param job_log_json_list: log json list
    :return: log json list with row count column
    """
    row_count_rows = df.groupby("load_id").count().withColumnRenamed("count", "row_count").collect()
    rows_dict_list = sorted(list(map(lambda x: x.asDict(), row_count_rows)), key=lambda x: x.get("load_id"))
    return list(map(lambda x: {**x[0], **x[1]}, zip(sorted(job_log_json_list, key=lambda x: x.get("load_id")),
                                                    rows_dict_list)))


def get_cols_from_s3_parquet(spark, s3_path):
    return spark.read.parquet(s3_path).columns


def get_glue_catalog_columns(database_name, table_name):
    """When the Glue catalog is updated, you can get the latest catalog columns right away
    using the Glue service, the Athena service has a delay in retrieving.
    """
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    except glue_client.exceptions.EntityNotFoundException as e:
        LOG.warning(f"Table {database_name}.{table_name} not found in Glue DataCatalog")
        return []
    columns = response["Table"]["StorageDescriptor"]["Columns"]
    partition_keys = response["Table"].get("PartitionKeys", [])
    columns = [c["Name"] for c in columns]
    partition_keys = [k["Name"] for k in partition_keys]
    columns += partition_keys
    return columns
