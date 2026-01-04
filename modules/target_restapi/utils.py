"""
REST API 目标端工具函数模块

提供 REST API 目标端相关的工具函数，包括缓存目录管理、S3 操作、文件读取和数据转换等功能
"""
import logging
import os
import sys
from itertools import zip_longest
from pathlib import Path
from typing import Optional, Dict, Tuple, List
from urllib.parse import urlsplit

import botocore.exceptions
from modules.s3 import s3_parser_to_bucket_prefix, s3_client

LOG = logging.getLogger(__name__)


def get_cache_dir(app: str) -> Path:
    """Locate a platform-appropriate cache directory for application to use

    Does not ensure that the cache directory exists.
    """
    # Linux, Unix, AIX, etc.
    if os.name == "posix" and sys.platform != "darwin":
        # use ~/.cache if empty OR not set
        xdg = os.environ.get("XDG_CACHE_HOME", None) or os.path.expanduser("/tmp/cache")
        return Path(xdg, app)

    # Mac OS
    elif sys.platform == "darwin":
        return Path(os.path.expanduser("~"), f"Library/Caches/{app}")

    # Windows (hopefully)
    else:
        local = os.environ.get("LOCALAPPDATA", None) or os.path.expanduser(
            "~\\AppData\\Local"
        )
        return Path(local, app)


def parse_s3_uri(uri) -> Tuple[str, str]:
    parsed = urlsplit(uri)
    bucket = parsed.netloc
    key = parsed.path
    return bucket, key


class SafeS3Util:
    @staticmethod
    def s3_object_exists(uri, client=s3_client):
        bucket, key = parse_s3_uri(uri)
        objects = client.list_objects_v2(Bucket=bucket, Prefix=key)
        return False if objects.get("KeyCount") == 0 else True

    @staticmethod
    def s3_copy(src_path, dst_path, client=s3_client, ignore_no_such_key=False) -> bool:
        src_bucket, src_key = parse_s3_uri(src_path)
        dst_bucket, dst_key = parse_s3_uri(dst_path)
        try:
            client.copy_object(
                Bucket=dst_bucket,
                CopySource={"Bucket": src_bucket, "Key": src_key},
                Key=dst_key,
                MetadataDirective="REPLACE",
            )
            return True
        except client.exceptions.NoSuchKey as e:
            if ignore_no_such_key:
                return False
            else:
                raise e

    @staticmethod
    def s3_head_object(uri, client=s3_client, ignore_not_found=False) -> Optional[Dict]:
        bucket, key = parse_s3_uri(uri)
        try:
            return client.head_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            if ignore_not_found and "Not Found" in str(e):
                return None
            raise e

    @staticmethod
    def localize_s3_uri(s3_path, folder="/tmp", is_file=False) -> str:
        _, filepath, filename = s3_parser_to_bucket_prefix(s3_path)
        d = folder / Path(filepath)
        if is_file:
            d = d.parent
            d.mkdir(parents=True, exist_ok=True)
            return str(d.joinpath(filename))
        d.mkdir(parents=True, exist_ok=True)
        return str(d)


def chunked_read_file(count: int, f_obj):
    for lines in zip_longest(*[enumerate(f_obj)] * count, fillvalue=None):
        zips = filter(lambda x: x is not None, lines)
        _, lines = zip(*zips)
        yield lines


def create_post_data(record: Dict, request_fields: List[str]) -> Dict:
    data = {}
    for field in request_fields:
        data[field] = record[field.lower()]
    return data
