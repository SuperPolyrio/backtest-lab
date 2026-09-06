"""Relocate an explicit list of completed experiments, preserving hashes/restore paths."""

import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path


RUNS = (
    "primary_1d_hotpath_v2", "primary_3d_malloc_trim", "primary_15d_recycled",
    "primary_30d_recycled", "primary_60d_recycled",
)
SCRIPTS = (
    "compare_real_execution_models.py", "compare_stratified_real_execution_models.py",
    "compare_pml2_event_driven_real_markets.py",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    source, archive = args.source.resolve(), args.archive.resolve()
    if archive.is_relative_to(source):
        parser.error("The recovery archive must be outside the source repository")
    paths = [source / "runtime_outputs/unified_fill_only_acceleration/benchmarks" / n for n in RUNS]
    paths += [source / "scripts" / n for n in SCRIPTS]
    paths = [p for p in paths if p.exists()]
    for path in paths:
        if path.is_symlink():
            raise RuntimeError(f"Refusing archive symlink: {path}")
        if path.is_dir():
            receipt = json.loads((path / "run_receipt.json").read_text())
            if not receipt.get("complete"):
                raise RuntimeError(f"Run is not complete: {path}")
    # Refuse changes to files currently opened by one of this user's processes.
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            if proc.stat().st_uid != os.getuid():
                continue
            for fd in (proc / "fd").iterdir():
                try:
                    opened = fd.resolve(strict=True)
                except (FileNotFoundError, PermissionError, OSError):
                    continue
                if any(opened == p or opened.is_relative_to(p) for p in paths):
                    raise RuntimeError(f"Active file under {opened}; PID {proc.name}")
        except (PermissionError, FileNotFoundError, ProcessLookupError):
            continue
    records = []
    for path in paths:
        for file in ([path] if path.is_file() else sorted(path.rglob("*"))):
            if not file.is_file():
                continue
            if file.is_symlink():
                raise RuntimeError(f"Refusing nested symlink: {file}")
            with file.open("rb") as stream:
                sha = hashlib.file_digest(stream, "sha256").hexdigest()
            records.append({"path": str(file.relative_to(source)), "bytes": file.stat().st_size, "sha256": sha})
    receipt = {"source": str(source), "archive": str(archive), "applied": args.apply,
               "bytes": sum(r["bytes"] for r in records), "files": records,
               "restore": "Move each archived relative path back under source; original hashes are recorded."}
    if args.apply:
        if (archive / "cleanup.json").exists():
            raise RuntimeError("Choose a new archive directory; do not overwrite a recovery receipt")
        archive.mkdir(parents=True, exist_ok=True)
        (archive / "cleanup.json").write_text(json.dumps(receipt, indent=2) + "\n")
        for path in paths:
            dest = archive / path.relative_to(source)
            if dest.exists():
                raise RuntimeError(f"Archive collision: {dest}")
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(path), str(dest))
    print(json.dumps({"paths": len(paths), "files": len(records), "bytes": receipt["bytes"], "applied": args.apply}))


if __name__ == "__main__":
    main()
