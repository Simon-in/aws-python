"""
Write-Ahead Log (WAL) 模块

提供 Write-Ahead Log 功能，支持本地文件和 S3 存储两种实现，用于可靠地记录和同步数据
"""
import json
import os.path
from pathlib import Path
from typing import Dict, Union, Callable

from modules.s3 import (
    s3_download,
    s3_upload,
)
from modules.target_restapi.utils import SafeS3Util


class AbstractWAL:
    def __init__(self, filename):
        self.filename = filename
        self._file = None

    def open(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def append(self, record: Union[Dict, str], hook: Callable = None):
        if self._file is None:
            raise IOError(f"Couldn't append to a unopened WAL file {self.filename}")

        if callable(hook):
            record = hook(record)
        if isinstance(record, dict):
            record = json.dumps(record, ensure_ascii=False, default=str)
        self._file.write(record + "\n")

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class LocalWAL(AbstractWAL):
    def open(self):
        Path(self.filename).parent.mkdir(parents=True, exist_ok=True)
        if self._file is None:
            self._file = open(self.filename, "a+", encoding="utf-8")

    def close(self):
        if self._file is not None:
            self._file.close()


class S3WAL(AbstractWAL):
    def __init__(self, filename, cache_dir="/tmp"):
        super(S3WAL, self).__init__(filename)
        assert filename.startswith("s3://"), f"Invalid s3 uri: {filename}"
        self.cache_dir = cache_dir
        self.local_file = None

    def _make_local_file(self):
        self.local_file = SafeS3Util.localize_s3_uri(
            self.filename, self.cache_dir, is_file=True
        )

    @staticmethod
    def file_sync(src_path, dst_path):
        if src_path.startswith("s3://") and Path(dst_path).parent.is_dir():
            s3_download(src_path, dst_path)
        elif Path(src_path).parent.is_dir() and dst_path.startswith("s3://"):
            s3_upload(src_path, dst_path)
        else:
            raise ValueError(
                f"Cannot sync file between {src_path!r} and {dst_path!r}, please check path"
            )

    def open(self):
        if not self.local_file:
            self._make_local_file()
            # self.file_sync(self.filename, self.local_file)
        if self._file is None:
            self._file = open(self.local_file, "a+", encoding="utf-8")

    def close(self):
        if self._file is not None:
            self._file.close()
        if Path(self.local_file).is_file() and os.path.getsize(self.local_file) > 0:
            self.file_sync(self.local_file, self.filename)
