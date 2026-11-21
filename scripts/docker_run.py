#!/usr/bin/env python3
"""Cross-platform scorer-style runner: docker_run.py DATA_DIR OUTPUT_DIR."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def main():
    data, output = (Path(arg).resolve() for arg in sys.argv[1:])
    if not data.is_dir():
        sys.exit(f"input directory does not exist: {data}")
    output.mkdir(parents=True, exist_ok=True)
    docker = os.environ.get("DOCKER") or shutil.which("docker")
    if not docker:
        sys.exit("docker not found; add Docker Desktop's resources/bin to PATH or set DOCKER")
    os.environ["PATH"] = str(Path(docker).parent) + os.pathsep + os.environ.get("PATH", "")
    cpus = os.environ.get("CPUS", "8")
    memory = os.environ.get("MEM", "24g")
    with tempfile.TemporaryDirectory(prefix="shopping-sessions-") as scratch:
        if os.name != "nt":
            output.chmod(0o777)
            Path(scratch).chmod(0o777)
        command = [
            docker, "run", "--rm", "--platform", os.environ.get("PLATFORM", "linux/amd64"),
            "--network", "none", "--read-only", "--tmpfs", "/tmp:rw,size=64m,mode=1777",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--pids-limit", "4096",
            "--cpus", cpus, "--memory", memory, "--memory-swap", memory, "--user", "1000:1000",
            "--mount", f"type=bind,source={data},target=/data,readonly",
            "--mount", f"type=bind,source={output},target=/output",
            "--mount", f"type=bind,source={scratch},target=/scratch",
            "-e", "DATA_DIR=/data", "-e", "OUTPUT_DIR=/output", "-e", "SCRATCH_DIR=/scratch",
            "-e", "HOME=/scratch", "-e", f"NCPU={int(float(cpus))}",
        ]
        if "PROFILE" in os.environ:
            command += ["-e", "PROFILE=1"]
        command.append(os.environ.get("IMAGE", "shopping-sessions:latest"))
        return subprocess.call(command)


if __name__ == "__main__":
    sys.exit(main())
