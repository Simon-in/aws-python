import os
from pathlib import Path
from shutil import which

from modules.rclone.utils import LOG, execute_command

__all__ = ["Bin"]


class Bin:
    def __init__(
        self,
        name: str,
        url: str,
        binary_path=None,
    ):
        self.name = name
        self.url = url
        self._binary_path = binary_path
        if not self.is_installed():
            self.install()

    @property
    def binary_path(self):
        return self._binary_path

    def is_installed(self) -> bool:
        """
        :return: True if rclone is correctly installed on the system.
        """
        if which(self.name) is not None:
            self._binary_path = self.name
            return True

        if self.executable():
            return True

        self._binary_path = self._binary_path or os.path.join(
            f"/tmp/opt/{self.name}/bin", self.name
        )
        return False

    def _s3_download(self, file_url, file_path):
        cmd = ["aws", "s3", "cp", file_url, file_path]
        ret = execute_command(cmd, print_output=False)
        if ret != 0:
            raise OSError(f"Download failed")

    def _http_download(self, file_url, file_path):
        raise NotImplementedError

    def exists(self, file_path):
        _bin = Path(file_path)
        if _bin.exists() and _bin.is_file():
            return True
        return False

    def executable(self):
        if self.exists(self.binary_path) and os.access(self.binary_path, os.X_OK):
            return True
        return False

    def download_bin(self):
        if self.exists(self.binary_path):
            return

        LOG.info(f"Downloading binary file to {self.binary_path}")
        Path(self.binary_path).parent.mkdir(parents=True, exist_ok=True)
        if self.url.startswith("s3://"):
            self._s3_download(self.url, self.binary_path)
        elif self.url.startswith("http://") or self.url.startswith("https://"):
            self._http_download(self.url, self.binary_path)
        else:
            raise ValueError(f"Do not support file url {self.url} to download")

    def install(self):
        LOG.info(f"Installing {self.name}")
        if not self.exists(self.binary_path):
            self.download_bin()
        if not self.executable():
            os.chmod(self.binary_path, 0o755)

    def execute(self, *args, **kwargs):
        raise NotImplementedError
