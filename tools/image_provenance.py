#!/usr/bin/env python3
"""Record immutable provenance for the exact container exercised by ART."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time


def docker_json(args: list[str]) -> dict:
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout).strip())
    value = json.loads(result.stdout)
    return value[0] if isinstance(value, list) else value


def collect(container: str) -> dict:
    container_doc = docker_json(["inspect", container])
    image_id = container_doc.get("Image")
    if not image_id:
        raise RuntimeError("container inspect did not return an image ID")
    image_doc = docker_json(["image", "inspect", image_id])
    repo_digests = sorted(image_doc.get("RepoDigests") or [])
    digest = repo_digests[0].split("@", 1)[-1] if repo_digests else image_id
    labels = (image_doc.get("Config") or {}).get("Labels") or {}
    env = {}
    for item in (container_doc.get("Config") or {}).get("Env") or []:
        if "=" in item:
            key, value = item.split("=", 1)
            env[key] = value

    addon_path = env.get("RUST_IVM_ADDON_PATH")
    addon_sha256 = None
    if addon_path:
        hashed = subprocess.run(
            ["docker", "exec", container, "sha256sum", addon_path],
            capture_output=True, text=True, timeout=30,
        )
        if hashed.returncode == 0 and hashed.stdout.strip():
            addon_sha256 = hashed.stdout.split()[0]

    return {
        "schema": 1,
        "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "container": container,
        "configured_image": (container_doc.get("Config") or {}).get("Image"),
        "image_id": image_id,
        "image_digest": digest,
        "repo_digests": repo_digests,
        "revision": labels.get("org.opencontainers.image.revision"),
        "addon_path": addon_path,
        "addon_sha256": addon_sha256,
        "runtime_flags": {
            key: env.get(key)
            for key in (
                "USE_RUST_IVM",
                "RUST_IVM_TSFN_QUEUE",
                "RUST_IVM_STREAM_CREDIT",
                "ZERO_NUM_SYNC_WORKERS",
                "ZERO_ROUND_ROBIN_ROUTING",
                "ZERO_CURSOR_PAGE_SIZE",
            )
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--container", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    try:
        report = collect(args.container)
    except Exception as error:
        print(f"ERROR: cannot record image provenance: {error}", file=sys.stderr)
        return 2
    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    with open(args.out, "w") as out:
        json.dump(report, out, indent=2)
    print(
        f"image provenance: {report['image_digest']} "
        f"revision={report['revision'] or 'unknown'} -> {args.out}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
