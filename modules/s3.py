"""
S3 存储服务操作模块

提供 S3 存储服务的各种操作功能，包括路径解析、对象上传下载、文件处理、异步请求等
"""
import asyncio
import os.path
from pathlib import Path
from typing import List, Dict
from urllib.parse import urlparse

import aiohttp
import requests
from modules.client import client, logger, chunked_iterable
from modules import excel_handler
from modules.conf import ConfigGlobal
from modules.awsglue_patch import openpyxl_wildcard_issue_monkey_set

import re
import boto3
import pandas
import io
from botocore.exceptions import ClientError
from zipfile import ZipFile

s3_client = client("s3")
s3_resource = boto3.resource("s3", region_name="cn-north-1")
LOG = logger()


def s3_parser_to_bucket_prefix(s3_path):
    """
    parse a full s3 path to bucket and prefix
    :param s3_path:  s3 full path string
    :return: bucket, prefix
    """
    s3_pattern = "^s3://([^/]+)/(.*?([^/]+)/?)$"
    re_result = re.match(s3_pattern, s3_path)
    if re_result:
        return re_result.groups()
    else:
        LOG.error(f"Invalid s3 path found: {s3_path}")
        raise Exception("Exception: Invalid s3 path expression")


def get_load_id_func(s3_url):
    """
    get load_id by regular expression from s3 key string
    :param s3_url: s3 object key string
    :return: load_id
    """
    load_id = re.match(r".*=((\d){14})/", s3_url)
    assert load_id, f"No correct load_id found in s3 uri: {s3_url}"
    return load_id[1]


def check_if_exist_obj(bucket, prefix):
    """
    check if a s3 path have objects
    :param bucket: s3 bucket name
    :param prefix: s3 prefix name
    :return: bool
    """
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return False if objects.get("KeyCount") == 0 else True


def list_s3_objs(s3_path: str, suffix=None):
    bucket, prefix, file_nm = s3_parser_to_bucket_prefix(s3_path)
    s3_response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    obj_list = [obj.get("Key") for obj in s3_response.get("Contents", [])]
    while s3_response.get("IsTruncated"):
        s3_response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix,
                                                ContinuationToken=s3_response.get("NextContinuationToken"))
        obj_list += [obj.get("Key") for obj in s3_response.get("Contents", [])]
    if isinstance(suffix, str):
        return [f"s3://{bucket}/{obj}" for obj in obj_list if obj.endswith(suffix)]
    elif isinstance(suffix, list):
        return [f"s3://{bucket}/{obj}" for obj in obj_list if obj.split(".")[-1] in suffix]
    elif suffix is None:
        return [f"s3://{bucket}/{obj}" for obj in obj_list]
    else:
        LOG.error(f"Invalid object suffix found: {suffix}")
        raise ClientError


def s3_list_objects(s3_path: str, suffix: str = None, reverse=True) -> List[Dict]:
    bucket, prefix, _ = s3_parser_to_bucket_prefix(s3_path)
    res = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = res.get("Contents", [])
    while res.get("IsTruncated"):
        res = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            ContinuationToken=res.get("NextContinuationToken"),
        )
        contents += res.get("Contents", [])
    if suffix:
        contents = [x for x in contents if x["Key"].endswith(suffix)]
    contents.sort(key=lambda x: x["LastModified"], reverse=reverse)
    return contents


def s3_list_folders(s3_path: str) -> List[str]:
    bucket, prefix, _ = s3_parser_to_bucket_prefix(s3_path)
    res = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    contents = res.get("CommonPrefixes", [])
    while res.get("IsTruncated"):
        res = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter="/",
            ContinuationToken=res.get("NextContinuationToken"),
        )
        contents += res.get("CommonPrefixes", [])
    folders = ["s3://" + os.path.join(bucket, x["Prefix"]) for x in contents]
    return folders


def s3_copy(src_path, dst_path):
    src_bucket, src_key, _ = s3_parser_to_bucket_prefix(src_path)
    dst_bucket, dst_key, _ = s3_parser_to_bucket_prefix(dst_path)
    s3_client.copy_object(
        Bucket=dst_bucket,
        CopySource={'Bucket': src_bucket, 'Key': src_key},
        Key=dst_key,
        MetadataDirective='REPLACE'
    )


def s3_upload(src_path: str, dst_path: str):
    """Upload files or directories to S3 bucket"""
    bucket, dst_path, _ = s3_parser_to_bucket_prefix(dst_path)
    src_path = Path(src_path).resolve()
    if src_path.is_file():
        s3_client.upload_file(str(src_path), bucket, dst_path)
    elif src_path.is_dir():
        for local_path in src_path.glob("**/*"):
            if not local_path.is_file():
                continue
            s3_path = Path(dst_path) / local_path.relative_to(src_path)
            s3_client.upload_file(str(local_path), bucket, str(s3_path))


def s3_download(src_path: str, dst_path: str):
    """Download files or directories from S3 bucket"""
    bucket, src_path, _ = s3_parser_to_bucket_prefix(src_path)
    dst_path = Path(dst_path).resolve()
    if not dst_path.exists():
        dst_path.mkdir(parents=True, exist_ok=True)

    res = s3_client.list_objects_v2(Bucket=bucket, Prefix=src_path)
    contents = res.get("Contents", [])
    if not contents:
        return
    for obj in contents:
        obj_path = obj["Key"]
        local_path = dst_path.joinpath(dst_path, os.path.relpath(obj_path, src_path))
        s3_client.download_file(bucket, obj_path, str(local_path))


def s3_delete_object(s3_path):
    bucket, prefix, _ = s3_parser_to_bucket_prefix(s3_path)
    s3_client.delete_object(Bucket=bucket, Key=prefix)


def s3_delete_folder(s3_path):
    bucket, prefix, _ = s3_parser_to_bucket_prefix(s3_path)
    s3_resource.Bucket(bucket).objects.filter(Prefix=prefix).delete()


def is_s3_path_empty(s3_uri):
    bucket, prefix, _ = s3_parser_to_bucket_prefix(s3_uri)
    objects = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
    return not bool(objects.get("Contents", []))


def get_s3_file_size(bucket: str, key: str) -> int:
    """
    get s3 object key size
    :param bucket: s3 bucket name
    :param key: s3 object key
    :return: key size
    """
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        if response:
            file_size = int(response.get('ResponseMetadata').get('HTTPHeaders').get('content-length'))
            return file_size
    except ClientError:
        LOG.exception(f'Client error reading S3 file {bucket} : {key}')
        raise ClientError


def s3_copy_func(source_bucket, source_key, target_bucket, target_key) -> None:
    """
    s3 key copy function
    :param source_bucket: source s3 bucket name
    :param source_key: source s3 bucket name
    :param target_bucket: target s3 bucket name
    :param target_key: target s3 bucket name
    """
    try:
        LOG.info(f"Fetch content of key: s3://{source_bucket}/{source_key}")
        # if s3 key size is smaller than 100M then put_object else download to local disk and upload it to s3
        if get_s3_file_size(source_bucket, source_key) < 100 * 1024 * 1024:
            obj_body = s3_client.get_object(Bucket=source_bucket, Key=source_key).get("Body")
            s3_client.put_object(Body=obj_body.read(), Bucket=target_bucket, Key=target_key)
        else:
            file_name = "/tmp/" + source_key.split("/")[-1]
            with open(file_name, 'wb') as data:
                s3_client.download_fileobj(source_bucket, source_key, data)
            with open(file_name, "rb") as data:
                s3_client.upload_fileobj(data, target_bucket, target_key)
        if not check_size_validation(source_bucket, source_key, target_bucket, target_key):
            # will add retry recursive function in future here
            raise Exception(f"Exception: Different file size found after object copied :{source_key} and {target_key}")
    except ClientError as e:
        LOG.warning(f"s3://{source_bucket}/{source_key} copy to s3://{target_bucket}/{target_key} failed.")
        raise e


def check_size_validation(source_bucket, source_key, target_bucket, target_key):
    """
    :param source_bucket: source s3 bucket name
    :param source_key: source s3 object prefix
    :param target_bucket: target s3 bucket
    :param target_key: target s3 object prefix
    :return: True : source object is same whit target object , False : source object is not same whit target object
    """
    source_key_contents = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_key)
    source_key_etag = [key.get("Size") for key in source_key_contents.get("Contents")]
    target_key_contents = s3_client.list_objects_v2(Bucket=target_bucket, Prefix=target_key)
    target_key_etag = [key.get("Size") for key in target_key_contents.get("Contents")]
    return source_key_etag == target_key_etag


def get_incremental_prefix(bucket, prefix, latest_load_id):
    """
    get an incremental list of s3 prefix by latest load_id
    :param bucket: s3 bucket name
    :param prefix: s3 prefix name
    :param latest_load_id: latest load id get from s3 common prefix
    :return: incremental s3 prefix list
    """
    prefix_list = []
    lz_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    for item in lz_objects.get("CommonPrefixes", []):
        prefix_list.append(item.get("Prefix"))
    while lz_objects["IsTruncated"]:
        continue_key = lz_objects["NextContinuationToken"]
        lz_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/",
                                               ContinuationToken=continue_key)
        for item in lz_objects.get("CommonPrefixes"):
            prefix_list.append(item.get("Prefix"))
    return sorted(list(filter(lambda x: get_load_id_func(x) > latest_load_id, prefix_list)),
                  key=lambda x: get_load_id_func(x))


def s3_source_excel_transfer(
        lz_folder: str, sheet_name, ext: str = "xlsx", **kwargs
):
    """
    :param lz_folder: s3 folder path
    :param sheet_name: Excel worksheet name, multiples separated by ","
    :param ext: Excel file extension, one of "xlsx", "xlsm", "xlsb", "xls"
    :param kwargs:
    """
    skip_row = int(kwargs.get("skip_row")) if kwargs.get("skip_row") is not None else kwargs.get("skip_row")
    use_cols = kwargs.get("use_cols")
    excel_dtypes = kwargs.get("excel_dtypes")  # int64, int32, float64, float32, datatime64
    bucket = s3_parser_to_bucket_prefix(lz_folder)[0]
    prefix = s3_parser_to_bucket_prefix(lz_folder)[1]
    result_dict = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    excel_objs = [
        obj["Key"] for obj in result_dict.get('Contents', []) if obj["Key"].split(".")[-1].lower() == ext
    ]
    if excel_objs.__len__() > 0:
        openpyxl_wildcard_issue_monkey_set()
        for key in excel_objs:
            for sht_nm in sheet_name.split(","):
                obj_content = s3_client.get_object(Bucket=bucket, Key=key)
                body = obj_content['Body']
                excel_string = body.read()
                df = excel_handler.read_excel(
                    io.BytesIO(excel_string),
                    sheet_name=sht_nm,
                    usecols=use_cols,
                    skiprows=skip_row,
                    keep_default_na=False
                )
                dtypes = dict(zip(list(df.columns), ["object" for i in range(len(list(df.columns)))]))
                excel_dtypes = excel_dtypes if excel_dtypes is not None else {}
                dtypes.update(excel_dtypes)
                df = excel_handler.read_excel(
                    io.BytesIO(excel_string),
                    sheet_name=sht_nm,
                    usecols=use_cols,
                    skiprows=skip_row,
                    keep_default_na=False,
                    dtype=dtypes
                )
                for col_nm, ty in df.dtypes.to_dict().items():
                    if "datetime64" in str(ty):
                        df[col_nm] = df[col_nm].dt.round("S")
                df = df.astype(str)
                df.replace({"NaT": None}, inplace=True)
                df.replace({"NaN": None}, inplace=True)
                df.columns = df.columns.map(lambda x: x.replace('\r', '').replace('\n', ''))
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, encoding='utf-8', sep="\1", quotechar="\2", line_terminator="\3")
                file_name = f"sheet/" + key.split("/")[-1].split(".")[0] + "_" + sht_nm + '.csv'
                csv_key = prefix + file_name if prefix.endswith("/") else prefix + '/' + file_name
                LOG.info("prepare to write {}".format(csv_key))
                s3_resource.Object(bucket, csv_key).put(Body=csv_buffer.getvalue())

    else:
        LOG.error(f"No valid Excel object found in s3 path: '{lz_folder}'")
        raise Exception("Valid Entity Not Found Exception")


def s3_source_csvs_to_excel(src_csv_map: Dict, tgt_xlsx_path, delimiter=","):
    """

    :param src_csv_map: source csv path and {"s3://xxx/test.csv": "Sheet1"}
    :param tgt_xlsx_path: target xlsx s3 uri
    :param delimiter: csv delimiter
    :return:
    """

    tgt_bucket, tgt_key, tgt_file_name = s3_parser_to_bucket_prefix(tgt_xlsx_path)
    df_list = list()
    for src_csv_uri, sheet_nm in src_csv_map.items():
        src_bucket, src_key, src_file_name = s3_parser_to_bucket_prefix(src_csv_uri)
        csv_obj_body = s3_client.get_object(Bucket=src_bucket, Key=src_key)["Body"].read()
        df_list.append(
            (pandas.read_csv(io.BytesIO(csv_obj_body), delimiter=delimiter, index_col=0), sheet_nm)
        )

    with io.BytesIO() as output:
        with pandas.ExcelWriter(output, engine='xlsxwriter') as writer:
            for df_tup in df_list:
                df_tup[0].to_excel(writer, df_tup[1])
        s3_resource.Object(tgt_bucket, tgt_key).put(Body=output.getvalue())


def s3_unzip_func(lz_s3_path: str, file_inside_zip_pattern: str):
    bucket, prefix, _ = s3_parser_to_bucket_prefix(lz_s3_path)
    result_dict = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if result_dict.get('Contents'):
        obj_list = result_dict['Contents']
        key_list = [obj['Key'] for obj in obj_list if obj['Key'].endswith('zip') or obj['Key'].endswith('7z')]
        for key in key_list:
            if key.endswith("zip"):
                obj = s3_resource.Object(bucket_name=bucket, key=key)
                buffer = io.BytesIO(obj.get()["Body"].read())
                zip_buffer = ZipFile(buffer)
                for file in zip_buffer.namelist():
                    if re.match(file_inside_zip_pattern, file):
                        open_file = zip_buffer.open(file)
                        s3_client.upload_fileobj(io.BytesIO(open_file.read()), bucket, prefix + 'unzip_file/' + file)
                        open_file.close()
                    else:
                        LOG.warning(f"File pattern not match: {file}")
                        continue

    else:
        LOG.error(f"Exception: Empty Path, no object found of {lz_s3_path}")
        raise ClientError



def http_to_s3_sync(url, bucket, key, proxy=None):
    resp = requests.get(url, stream=True, proxies=proxy)
    if not resp.ok:
        if "expired" in resp.content.decode("utf-8").lower():
            raise Exception(f"Download url is expired: {resp.url}")
        else:
            resp.raise_for_status()
    local_path = '/tmp/' + key.replace("/", "_")

    try:
        s3_client.upload_fileobj(io.BytesIO(resp.content), bucket, key)
    except ClientError:
        LOG.info(f"Prepare to download to local file: {local_path}")
        with open(local_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        LOG.info(f"Download completed, start to upload to s3://{bucket}/{key}")
        s3_client.upload_file(local_path, bucket, key)
        LOG.info(f"Remove local file {local_path}")
        Path(local_path).unlink()
    LOG.info(f"{url} has been load to s3://{bucket}/{key} successfully!")


async def _http_to_s3_sync(url, session, key, s3_path=ConfigGlobal.unstructured_data_location):
    timeout = 3600
    local_path = '/tmp/' + key.replace("/", "_")
    bucket, prefix, _ = s3_parser_to_bucket_prefix(s3_path)
    LOG.info(f"Start to process url: {url} to s3://{bucket}/{prefix}{key}")
    is_success = True
    async with session.get(url, timeout=timeout) as response:
        try:
            data = await response.content.read()
            if response.ok:
                try:
                    s3_client.upload_fileobj(io.BytesIO(data), bucket, prefix + key)
                except ClientError as e:
                    LOG.warning(e)
                    with open(local_path, 'rb') as f:
                        f.write(data)
                    s3_client.upload_file(local_path, bucket, prefix + key)
                    Path(local_path).unlink()
                LOG.info(f"Data has been process to s3://{bucket}/{prefix}{key}")
            else:
                if "expired" in data.decode("utf-8").lower():
                    LOG.warning(f"Download url is expired: {url}")
                else:
                    LOG.error(f"Encountering error and information shows below:  "
                              f"code: {response.status}, \n"
                              f"reason: {response.reason}, \n"
                              f"headers: {response.raw_headers}."
                              )
                    is_success = False

        except Exception as e:
            LOG.error(e)
            is_success = False

    return is_success, {"url": url, "key": key}



async def http_to_s3_async(url_list, max_workers=10, recursion_depth=3):
    """
    :param recursion_depth: recursion depth
    :param url_list:  [{"url": "https://xxx", "key":"2023/10/01/xxx.wav"}]
    :param max_workers: tcp max connection number
    :return:
    """

    tcp_connection = aiohttp.TCPConnector(limit=max_workers)
    failed_tasks = []
    return_task = []
    chunk_index = 1
    total_url = '\n'.join([url["url"] + " to " + url["key"] for url in url_list])
    LOG.info(f"Total download objects length is: {len(url_list)}")
    LOG.info(f"Total download objects list: {total_url}")

    async with aiohttp.ClientSession(connector=tcp_connection) as session:
        for url_chunk in chunked_iterable(url_list, max_workers):
            tasks = []
            for url_info in url_chunk:
                task = asyncio.ensure_future(_http_to_s3_sync(url_info["url"], session, url_info["key"]))
                tasks.append(task)
            LOG.info(f"Start {chunk_index} batch async download jobs!")
            chunk_res = await asyncio.gather(*tasks, return_exceptions=True)
            LOG.info(f"Chunk execution result: {chunk_res}")
            chunk_index += 1
            failed_tasks.extend([tup[1] for tup in chunk_res if not tup[0]])
    if failed_tasks:
        if recursion_depth > 0:
            await http_to_s3_async(failed_tasks, max_workers=5, recursion_depth=recursion_depth-1)
        else:
            LOG.warning(f"Can't download below urls after retries: {failed_tasks}")
            return_task.extend(failed_tasks)
    await tcp_connection.close()
    return return_task


class S3Url(object):
    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        if self._parsed.query:
            return self._parsed.path.lstrip("/") + "?" + self._parsed.query
        else:
            return self._parsed.path.lstrip("/")

    @property
    def url(self):
        return self._parsed.geturl()
