#!/usr/bin/env python3
from __future__ import annotations

import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional


def run_and_profile() -> int:
    repo_root = Path(__file__).resolve().parent
    cmd: List[str] = [
        str(repo_root / "./cxl-util"),
        "start",
        "-c",
        "fm",
        "-c",
        "switch",
        "-c",
        "host-group",
        "-c",
        "sld-group",
        "--config-file",
        "configs/1vcs_4sld.yaml",
    ]

    env = os.environ.copy()

    proc = subprocess.Popen(
        cmd,
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        preexec_fn=os.setsid,
        env=env,
    )

    deadline = time.time() + 30.0
    perf_proc: Optional[subprocess.Popen] = None
    saw_write = False
    saw_read = False

    def list_pids_in_pgroup(pgid: int) -> List[int]:
        try:
            out = subprocess.check_output(
                ["bash", "-lc", f"ps -o pid= -g {shlex.quote(str(pgid))}"],
                cwd=str(repo_root),
                text=True,
            )
            pids = [int(x) for x in out.strip().split() if x.strip()]
            return sorted(set(pids))
        except Exception:
            return [proc.pid]

    def start_perf() -> None:
        nonlocal perf_proc
        if perf_proc is not None and perf_proc.poll() is None:
            return
        pgid = os.getpgid(proc.pid)
        pid_list = list_pids_in_pgroup(pgid)
        pid_arg = ",".join(str(p) for p in pid_list)
        perf_cmd = [
            "perf",
            "record",
            "-g",
            "--call-graph",
            "dwarf",
            "--inherit",
            "--all-user",
            "--no-bpf",
            "-e",
            "cpu-clock",
            "-F",
            "999",
            "--pid",
            pid_arg,
            "-o",
            str(repo_root / "perf.data"),
        ]
        perf_proc = subprocess.Popen(perf_cmd, cwd=str(repo_root))

    def stop_perf() -> None:
        nonlocal perf_proc
        if perf_proc is None:
            return
        if perf_proc.poll() is None:
            try:
                perf_proc.send_signal(signal.SIGINT)
                try:
                    perf_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    perf_proc.kill()
            except ProcessLookupError:
                pass

    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()

            if "PERF_START" in line:
                start_perf()
            if "Write RESULTS:" in line:
                saw_write = True
            if "Read RESULTS:" in line:
                saw_read = True
            if "PERF_END" in line or (saw_write and saw_read):
                stop_perf()
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGINT)
                except ProcessLookupError:
                    pass

            if time.time() > deadline:
                stop_perf()
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGINT)
                except ProcessLookupError:
                    pass
                break
    finally:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass

    report_path = repo_root / "perf_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        subprocess.run(
            ["perf", "report", "--stdio", "-g"], cwd=str(repo_root), check=False, stdout=f
        )

    return proc.returncode or 0


if __name__ == "__main__":
    sys.exit(run_and_profile())
