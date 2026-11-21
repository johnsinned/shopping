#!/usr/bin/env python3
"""Compare Linux images using Docker's container start/exit timestamps.

Use a Linux Docker volume to avoid Windows bind-mount I/O in the measured run:
  benchmark.py DATA_VOLUME --image BASELINE --image CANDIDATE --output-volume OUT_VOLUME
DATA_VOLUME contains a data/ directory. OUT_VOLUME must be writable by uid 1000.
"""
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_volume")
    parser.add_argument("--image", action="append", required=True)
    parser.add_argument("--output-volume", required=True)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--cpus", type=int, default=8)
    parser.add_argument("--memory", default="24g")
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()
    if args.repeats < 1 or args.warmups < 0 or args.cpus < 1:
        parser.error("repeats and cpus must be positive; warmups must be nonnegative")
    docker = os.environ.get("DOCKER") or shutil.which("docker")
    if not docker:
        parser.error("docker not found; add Docker Desktop's resources/bin to PATH or set DOCKER")
    os.environ["PATH"] = str(Path(docker).parent) + os.pathsep + os.environ.get("PATH", "")

    def call(*command):
        return subprocess.run([docker, *command], capture_output=True, text=True, check=True)

    image_ids = {image: call("image", "inspect", "--format", "{{.Id}}", image).stdout.strip()
                 for image in args.image}
    samples = {image: [] for image in args.image}
    for iteration in range(args.warmups + args.repeats):
        order = args.image if iteration % 2 == 0 else list(reversed(args.image))
        for image in order:
            output = f"/output/image-{args.image.index(image)}"
            cid = call(
                "create", "--platform", "linux/amd64", "--network", "none", "--read-only",
                "--tmpfs", "/tmp:rw,size=64m,mode=1777", "--cap-drop", "ALL",
                "--security-opt", "no-new-privileges", "--pids-limit", "4096",
                "--cpus", str(args.cpus), "--memory", args.memory, "--memory-swap", args.memory,
                "--user", "1000:1000", "--mount", f"type=volume,source={args.data_volume},target=/data,readonly",
                "--mount", f"type=volume,source={args.output_volume},target=/output",
                "-e", "DATA_DIR=/data/data", "-e", f"OUTPUT_DIR={output}",
                "-e", f"NCPU={args.cpus}", "-e", "PROFILE=1", image,
            ).stdout.strip()
            try:
                process = subprocess.run([docker, "start", "--attach", cid], capture_output=True, text=True)
                state = json.loads(call("inspect", "--format", "{{json .State}}", cid).stdout)
                if process.returncode or state["ExitCode"]:
                    raise RuntimeError(f"{image}: exit {state['ExitCode']}; OOM={state['OOMKilled']}\n{process.stderr}")
                seconds = (datetime.fromisoformat(state["FinishedAt"].replace("Z", "+00:00"))
                           - datetime.fromisoformat(state["StartedAt"].replace("Z", "+00:00"))).total_seconds()
                warmup = iteration < args.warmups
                print(f"{image}: {seconds:.6f}s{' (warmup)' if warmup else ''}; {process.stderr.strip()}", flush=True)
                if not warmup:
                    samples[image].append({"seconds": seconds, "profile": process.stderr.strip()})
            finally:
                call("rm", "--force", cid)
    medians = {image: statistics.median(s["seconds"] for s in runs) for image, runs in samples.items()}
    print("Median container start to exit:", json.dumps(medians, indent=2))
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        report = {"data_volume": args.data_volume, "cpus": args.cpus, "memory": args.memory,
                  "warmups": args.warmups, "repeats": args.repeats, "image_ids": image_ids,
                  "timing": "Docker State.StartedAt to State.FinishedAt",
                  "samples": samples, "medians": medians}
        args.json.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
