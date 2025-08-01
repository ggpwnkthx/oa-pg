import os
from pathlib import Path
from typing import List, Optional
from .utils import psql_base_args, psql_exec_sql, run, write_text, detect_loader_os


def generate_nation_loader_script(
    conn_str: str,
    output_script: Path,
    os_target: Optional[str],
    extra_psql_args: Optional[List[str]],
) -> None:
    print("== Generating national loader script ==")
    target = os_target or detect_loader_os()
    if target not in ("sh", "windows"):
        raise ValueError("os_target must be 'sh' or 'windows'")
    sql = f"SELECT loader_generate_nation_script('{target}');"
    script_body = run(
        psql_base_args(conn_str, extra_psql_args) + ["-A", "-t", "-c", sql],
        capture=True
    )
    write_text(output_script, script_body)
    print(f"✓ Wrote {output_script} ({len(script_body)} bytes)")


def execute_loader_script(script_path: Path) -> None:
    print("== Executing national loader script ==")
    if not script_path.exists():
        raise FileNotFoundError(f"Loader script not found: {script_path}")
    if os.name == "nt" or script_path.suffix.lower() == ".bat":
        cmd = ["cmd.exe", "/c", str(script_path)]
    else:
        cmd = ["bash", str(script_path)]
    run(cmd)
    print("✓ National tables loaded into tiger_data (per loader script)")


def post_load_maintenance(
    conn_str: str,
    extra_psql_args: Optional[List[str]],
    analyze: bool = True
) -> None:
    print("== Post-load maintenance ==")
    psql_exec_sql("SELECT install_missing_indexes();",
                  conn_str, extra_psql_args)
    print("✓ install_missing_indexes() completed")
    if analyze:
        psql_exec_sql("ANALYZE;", conn_str, extra_psql_args)
        print("✓ ANALYZE completed")
