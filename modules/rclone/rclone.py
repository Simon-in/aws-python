import os
import textwrap
from enum import Enum
from pathlib import Path
from shlex import quote
from subprocess import SubprocessError
from typing import Optional

from modules.rclone.bin import Bin
from modules.rclone.utils import (
    LOG,
    nio_execute_command,
    execute_command,
)
from modules.conf import ConfigGlobal
from modules.secret_manager import get_secret
import boto3
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
import base64


__all__ = ["RClone", "RemoteTypes", "Config"]


RCLONE = "rclone"
RCLONE_DOWNLOAD_URL = (
    f"s3://landing-{ConfigGlobal.env}-cn-north-1/retail/rclone/libs/bin/"
    f"rclone-v1.64.2-linux-amd64/rclone"
)
RCLONE_BIN_PATH = "/tmp/opt/rclone/bin/rclone"
RCLONE_CONFIG_PATH = "/tmp/opt/rclone/.config/rclone.conf"


cryptKey = type[int](
    [
        0x9c,
        0x8f,
        0x9a,
        0x9b,
        0x8e,
        0x99,
        0x98,
        0x97,
        0x96,
        0x95,
        0x94,
        0x93,
        0x92,
        0x91,
        0x90
    ]
)


def crypt(in_bytes, iv):
    cipher = AES.new(cryptKey, AES.MODE_CBC, iv, nonce=b"", initializer_value=iv)
    return cipher.encrypt(in_bytes)


def obscure(x):
    plaintext = x.decode("utf-8")
    iv = get_random_bytes(AES.block_size)
    ciphertext = crypt(plaintext, iv)
    return base64.urlsafe_b64encode(iv + ciphertext).decode("utf-8")


class RemoteTypes(str, Enum):
    """These are all the cloud systems support by rclone (generated with v1.65.2).
    A more detailed overview can be found here: https://rclone.org/overview/
    """

    amazon_cloud_drive = "amazon cloud drive"
    azureblob = "azureblob"
    azurefiles = "azurefiles"
    b2 = "b2"
    box = "box"
    cache = "cache"
    chunker = "chunker"
    combine = "combine"
    compress = "compress"
    crypt = "crypt"
    drive = "drive"
    dropbox = "dropbox"
    fichier = "fichier"
    filefabric = "filefabric"
    ftp = "ftp"
    google_cloud_storage = "google cloud storage"
    google_photos = "google photos"
    hasher = "hasher"
    hdfs = "hdfs"
    hidrive = "hidrive"
    http = "http"
    imagekit = "imagekit"
    internetarchive = "internetarchive"
    jottacloud = "jottacloud"
    koofr = "koofr"
    linkbox = "linkbox"
    local = "local"
    mailru = "mailru"
    mega = "mega"
    memory = "memory"
    netstorage = "netstorage"
    onedrive = "onedrive"
    opendrive = "opendrive"
    oracleobjectstorage = "oracleobjectstorage"
    pcloud = "pcloud"
    pikpak = "pikpak"
    premiumizeme = "premiumizeme"
    protondrive = "protondrive"
    putio = "putio"
    qingstor = "qingstor"
    quatrix = "quatrix"
    s3 = "s3"
    seafile = "seafile"
    sftp = "sftp"
    sharefile = "sharefile"
    sia = "sia"
    smb = "smb"
    storj = "storj"
    sugarsync = "sugarsync"
    swift = "swift"
    tardigrade = "tardigrade"
    union = "union"
    uptobox = "uptobox"
    webdav = "webdav"
    yandex = "yandex"
    zoho = "zoho"


class Config:
    def __init__(
        self,
        alias,
        secret,
    ):
        self.alias = alias
        _, self.properties = get_secret(secret)

    def s3(self):
        session = boto3.Session()
        credentials = session.get_credentials()
        ak = credentials.access_key
        sk = credentials.secret_key
        st = credentials.token
        return textwrap.dedent(
            f"""
            [{self.alias}]
            type = {RemoteTypes.s3.value}
            provider = AWS
            env_auth = false
            access_key_id = {ak}
            secret_access_key = {sk}
            session_token = {st}
            region = {ConfigGlobal.region}
            location_constraint = {ConfigGlobal.region}
        """
        )

    def blob(self):
        return textwrap.dedent(
            f"""
                [{self.alias}]
                type = {RemoteTypes.azureblob.value}
                env_auth = false
                account = {self.properties.get("account")}
                key = {self.properties.get("key")}
                endpoint = {self.properties.get("endpoint")}
            """
        )
    
    def sftp(self):
        obscure_password = obscure(self.properties.get("password"))
        return textwrap.dedent(
            f"""
                [{self.alias}]
                type = {RemoteTypes.sftp.value}
                host = {self.properties.get("host")}
                user = {self.properties.get("user")}
                password = {obscure_password}
                port = {self.properties.get("port")}
                md5sum_command = none
                sha1sum_command = none
            """
        )


class RClone(Bin):
    def __init__(self, config_path=RCLONE_CONFIG_PATH):
        super(RClone, self).__init__(RCLONE, RCLONE_DOWNLOAD_URL, RCLONE_BIN_PATH)
        self._config_path = config_path

    @property
    def config_path(self):
        return self._config_path

    def execute(
        self, *args, debug=False, call_check=False, non_blocking=False
    ) -> Optional[int]:
        if not self.is_installed():
            raise OSError(f"Cannot execute command {self.binary_path}, please check installation")
        if not self.is_configured():
            raise FileNotFoundError(f"Cannot found rclone config at {self.config_path}")

        cmd = [self.binary_path, f"--config={self.config_path}"]
        if debug:
            cmd.append("-vv")
        cmd += list(args)
        if non_blocking:
            ret = nio_execute_command(cmd)
        else:
            ret = execute_command(cmd)
        if call_check is False:
            return ret

        if ret != 0:
            raise SubprocessError(f"Error executing command {cmd}: {ret}")

    def sync(
        self, source, destination, debug=False, call_check=False, non_blocking=False
    ):
        params = ["sync", quote(source), quote(destination)]
        return self.execute(
            *params, debug=debug, call_check=call_check, non_blocking=non_blocking
        )

    def copy(
        self, source, destination, debug=False, call_check=False, non_blocking=False
    ):
        params = ["copy", quote(source), quote(destination)]
        return self.execute(
            *params, debug=debug, call_check=call_check, non_blocking=non_blocking
        )

    def ls(self, remote_path, debug=False, call_check=False, non_blocking=False):
        params = ["ls", quote(remote_path)]
        return self.execute(
            *params, debug=debug, call_check=call_check, non_blocking=non_blocking
        )

    def lsd(self, remote_path, debug=False, call_check=False, non_blocking=False):
        params = ["lsd", quote(remote_path)]
        return self.execute(
            *params, debug=debug, call_check=call_check, non_blocking=non_blocking
        )

    def is_configured(self):
        if self.exists(self.config_path) and os.path.getsize(self.config_path) > 0:
            return True
        return False

    def create_config(self, src_storage: str, dst_storage: str):
        if self.is_configured():
            LOG.info(f"rclone config found at: {self.config_path}")
            return

        LOG.info(f"Creating rclone config at {self.config_path}")
        rclone_conf = Path(self.config_path)
        rclone_conf.parent.mkdir(parents=True, exist_ok=True)
        config_content = f"{src_storage}\n{dst_storage}"
        rclone_conf.write_text(config_content)

        LOG.debug(f"rclone config:")
        LOG.debug(config_content)
        LOG.info(f"rclone configured")
