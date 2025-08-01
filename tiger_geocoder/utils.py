import os
import platform
import shlex
import subprocess
from shutil import which
from pathlib import Path
from typing import List, Optional


class CmdError(RuntimeError):
    pass


def run(cmd: List[str],
        env: Optional[dict] = None,
        check: bool = True,
        capture: bool = False,
        cwd: Optional[str] = None) -> str:
    """Run a command, optionally capturing output, with rich errors."""
    try:
        if capture:
            proc = subprocess.run(cmd, env=env, cwd=cwd,
                                  check=check, text=True,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
            return proc.stdout
        else:
            subprocess.run(cmd, env=env, cwd=cwd, check=check)
            return ""
    except subprocess.CalledProcessError as e:
        msg = (
            f"Command failed ({e.returncode}): {' '.join(shlex.quote(x) for x in cmd)}\n"
            f"STDOUT:\n{e.stdout or ''}\nSTDERR:\n{e.stderr or ''}"
        )
        raise CmdError(msg) from e


def assert_tool(name: str) -> None:
    """Verify that a CLI tool is on PATH."""
    if which(name) is None:
        raise CmdError(
            f"Missing required tool on PATH: '{name}'. Install it and re-run.")
    print(f"✓ Found tool: {name}")


def psql_base_args(conn_str: str,
                   extra_psql_args: Optional[List[str]] = None) -> List[str]:
    """
    Build a psql invocation around a single DSN string.
    Equivalent to: psql -v ON_ERROR_STOP=1 -d "<conn_str>" [extra args...]
    """
    args = ["psql", "-v", "ON_ERROR_STOP=1", "-d", conn_str]
    if extra_psql_args:
        args += extra_psql_args
    return args


def psql_exec_sql(sql: str,
                  conn_str: str,
                  extra_psql_args: Optional[List[str]] = None) -> str:
    """Run a single SQL command via psql and return its stdout."""
    cmd = psql_base_args(conn_str, extra_psql_args) + ["-c", sql]
    return run(cmd, capture=True)


def detect_loader_os() -> str:
    """Return 'sh' on POSIX, 'windows' on Win."""
    system = platform.system().lower()
    if "windows" in system:
        return "windows"
    return "sh"


def write_text(path: Path, content: str) -> None:
    """Write a text file, making parent dirs and setting +x on non‐Windows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    if os.name != "nt":
        try:
            mode = path.stat().st_mode
            path.chmod(mode | 0o111)
        except Exception:
            pass
