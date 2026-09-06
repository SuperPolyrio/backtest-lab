"""One-time, non-destructive PMQ extraction with an auditable file inventory."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import shutil
from pathlib import Path


def modules(root: Path) -> dict[str, Path]:
    result = {}
    for top in ("quant", "scripts", "sdk", "strategies", "tests", "experiments"):
        for path in (root / top).rglob("*.py"):
            if {"vendor", "__pycache__"}.intersection(path.parts):
                continue
            parts = list(path.relative_to(root).with_suffix("").parts)
            if parts[-1] == "__init__":
                parts.pop()
            result[".".join(parts)] = path
    return result


def dependencies(name: str, path: Path) -> set[str]:
    package = name.split(".") if path.name == "__init__.py" else name.split(".")[:-1]
    found = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                base = ".".join(package[:len(package) - node.level + 1] + ([base] if base else []))
            found.add(base)
            found.update(base + "." + alias.name for alias in node.names)
    return found


def closure(seeds: set[str], graph: dict[str, set[str]]) -> set[str]:
    found, todo = set(seeds), list(seeds)
    while todo:
        for dep in graph.get(todo.pop(), set()):
            if dep in graph and dep not in found:
                found.add(dep)
                todo.append(dep)
    return found


def backtest_blueprint(source: str) -> str:
    """Preserve existing route bodies, removing unrelated routes and dead helpers."""
    tree = ast.parse(source)
    factory = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "create_quant_blueprint")
    original_body = list(factory.body)
    prefixes = ("/fill-only/", "/prediction-l2/", "/backtest-", "/parameter-search-",
                "/strategy-", "/execution-profile-", "/production-parameter-")
    retained = []
    for node in factory.body:
        if isinstance(node, ast.FunctionDef) and node.decorator_list:
            routes = [d.args[0].value for d in node.decorator_list
                      if isinstance(d, ast.Call) and d.args and isinstance(d.args[0], ast.Constant)]
            if routes and not any(str(route).startswith(prefixes) for route in routes):
                continue
        retained.append(node)
    factory.body = retained

    def used(nodes: list[ast.stmt]) -> set[str]:
        return {n.id for node in nodes for n in ast.walk(node)
                if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}

    # Drop unused nested helper functions after deleting their route callers.
    while True:
        names = used(factory.body)
        reduced = [n for n in factory.body if not isinstance(n, ast.FunctionDef)
                   or n.decorator_list or n.name in names]
        if len(reduced) == len(factory.body):
            break
        factory.body = reduced
    definitions = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            definitions[node.name] = node
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    definitions[target.id] = node
    needed = {"create_quant_blueprint"}
    while True:
        expanded = needed | used([definitions[n] for n in needed if n in definitions])
        if expanded == needed:
            break
        needed = expanded
    output = []
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [a for a in node.names if (a.asname or a.name.split(".")[0]) in needed]
            if isinstance(node, ast.ImportFrom) and node.module == "__future__":
                names = node.names
            if names:
                node.names = names
                output.append(node)
        elif any(node is definitions.get(name) for name in needed):
            output.append(node)
    lines = source.splitlines(keepends=True)
    removed = set()
    for node in original_body:
        if node not in factory.body:
            start = min([node.lineno] + [d.lineno for d in getattr(node, "decorator_list", [])])
            removed.update(range(start, node.end_lineno + 1))
    # Preserve handler text exactly, including contract/source-audit strings.
    return '# Extracted from PMQ: existing backtest route contracts are unchanged.\n' + '\n'.join(
        ''.join(lines[i - 1] for i in range(node.lineno, node.end_lineno + 1) if i not in removed)
        for node in output
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    args = parser.parse_args()
    source, target = args.source.resolve(), args.target.resolve()
    if (target / "docs/migration/inventory.json").exists():
        parser.error("Extraction already exists; do not overwrite the reviewed migration")
    found = modules(source)
    direct = {name: dependencies(name, path) for name, path in found.items()}
    for name, path in found.items():
        direct[name].update(node.value for node in ast.walk(ast.parse(path.read_text()))
                            if isinstance(node, ast.Constant) and isinstance(node.value, str)
                            and node.value in found)
    graph = {name: set(deps) for name, deps in direct.items()}
    for name, deps in graph.items():
        parts = name.split(".")
        deps.update(".".join(parts[:i]) for i in range(1, len(parts)))
    runtime = {m for m, p in found.items() if m.startswith("quant.backtest") and "tests" not in p.parts}
    runtime |= {"quant.workers.backtest_runner", "quant.backtest_runner", "quant.backtest_engine"}
    runtime = closure(runtime, graph)
    # Retain reusable CLIs, but retire three superseded fixed-cohort comparisons.
    retired = {"scripts.compare_real_execution_models", "scripts.compare_stratified_real_execution_models",
               "scripts.compare_pml2_event_driven_real_markets"}
    scripts = {m for m, deps in direct.items() if m.startswith("scripts.")
               and not m.startswith("scripts.api.")
               and any(d.startswith("quant.backtest") for d in deps)
               and not any(d.startswith(("quant.paper", "quant.execution")) for d in deps)} - retired
    scripts |= {"scripts.run_unified_fill_only_replay", "scripts.quant_api_server"}
    scripts |= {"scripts.run_fill_only_v3_large_cross_validation",
                "scripts.explain_fill_only_v3_cross_validation_orders",
                "scripts.validate_pml2_order_contract_cross_validation"}
    # Tests are retained by dependency, not copied by directory name alone.
    tests = {m for m, p in found.items() if p.name.startswith("test_")
             and (any(d.startswith("quant.backtest") for d in direct[m])
                  or (p.parent == source / "tests" and "fill_only" in p.name))
             and not any(d.startswith("experiments.") for d in direct[m])
             and not any(d.startswith(("quant.paper", "quant.execution")) for d in direct[m])}
    selected = closure(runtime | scripts | tests, graph)
    selected.discard("scripts.api.product_hub_store")
    routes = Path("scripts/api/routes/quant.py")
    selected.discard("scripts.api.routes.quant")
    records = []

    def copy(path: Path, category: str) -> None:
        rel = path.relative_to(source)
        dest = target / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dest)
        records.append({"path": str(rel), "category": category, "bytes": path.stat().st_size,
                        "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest()})

    for name in sorted(selected):
        path = found.get(name)
        if path is None:
            continue
        category = "runtime" if name in runtime else "regression" if name in tests else "cli_or_dependency"
        copy(path, category)
        for parent in path.relative_to(source).parents:
            init = source / parent / "__init__.py"
            if str(parent) != "." and init.exists() and not (target / parent / "__init__.py").exists():
                copy(init, "package")
    dest = target / routes
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(backtest_blueprint((source / routes).read_text()))
    records.append({"path": str(routes), "category": "backtest_only_api_extraction"})
    trees = ["config/execution", "config/calibration", "configs/calibration", "quantV2/docs",
             "docs/回测", "docs/pnl", "docs/量化/成交模型", "docs/量化/别人的回测框架",
             "rust/fill_only_kernel"]
    for directory in trees:
        for path in sorted((source / directory).rglob("*")):
            if not path.is_file() or {"target", "__pycache__", ".pytest_cache"}.intersection(path.parts):
                continue
            copy(path, "configuration" if directory.startswith(("config", "rust")) else "documentation")
    for filename in ("environment-backtest.yml", "rust-toolchain.toml", "pytest.ini",
                     "docs/量化/phase1.md", "docs/量化/raw_orderfilled_replay与PMXT_L2.md",
                     "docs/量化/fill_first外部源配置说明.md", "docs/量化/回测总结.md",
                     "docs/量化/回测Xraw.md", "docs/量化/shadow_live_paired_execution方案.md",
                     "docs/api/fill-only-v2-replay-api.md", "docs/api/fill-only-v3-trade-only-api.md"):
        path = source / filename
        if path.exists():
            copy(path, "configuration_or_documentation")
    audit = target / "docs/migration"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "inventory.json").write_text(json.dumps({
        "source": str(source), "target": str(target), "files": records,
        "retired_fixed_cohort_scripts": sorted(retired),
        "excluded": ["credentials", "vendor/backtrader", "build caches", "raw archives", "historical experiment outputs"],
        "source_cleanup_performed": False,
    }, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({"runtime_modules": len(runtime), "selected_modules": len(selected),
                      "files": len(records), "bytes": sum(r.get("bytes", 0) for r in records)}))


if __name__ == "__main__":
    main()
