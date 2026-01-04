"""
Rclone 工具实用函数模块

提供 Rclone 工具的实用函数，包括命令执行、跨平台命令处理等功能
"""
import sys
import tempfile
from subprocess import Popen, PIPE, STDOUT
from typing import List

from modules.client import _logger

__all__ = ["execute_command", "nio_execute_command", "IS_WIN32", "IS_MACOS", "LOG"]

LOG = _logger()

IS_WIN32 = sys.platform == "win32"
IS_MACOS = sys.platform == "darwin"


def execute_command(cmd: List[str], *, print_output: bool = True) -> int:
    if IS_WIN32:
        shell = False
    else:
        cmd = " ".join(cmd)
        shell = True
    proc = Popen(
        cmd,
        stdout=PIPE,
        stderr=STDOUT,
        encoding="utf-8",
        errors="utf-8",
        shell=shell,
    )
    LOG.info(f"[DEBUG] command: {cmd}, pid: {proc.pid}")
    # live stream STDOUT
    while True:
        outs = proc.stdout.readline()
        if outs:
            if print_output:
                print(outs, end="")
        if proc.poll() is not None and outs == "":
            break
    return proc.returncode


def nio_execute_command(cmd: List[str]) -> int:
    """Avoid large outputs deadlocking program
    ref: https://thraxil.org/users/anders/posts/2008/03/13/Subprocess-Hanging-PIPE-is-your-enemy/
    """
    if IS_WIN32:
        shell = False
    else:
        cmd = " ".join(cmd)
        shell = True
    with tempfile.TemporaryFile() as tempf:
        proc = Popen(cmd, stdout=tempf, stderr=STDOUT, shell=shell)
        print(f"[DEBUG] command: {cmd}, pid: {proc.pid}")
        return_code = proc.wait()

        tempf.seek(0)
        for line in tempf:
            print(line.decode("utf8"), end="")

    return return_code
