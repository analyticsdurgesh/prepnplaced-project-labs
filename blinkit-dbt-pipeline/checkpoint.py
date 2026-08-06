#!/usr/bin/env python3
"""PrepNPlaced Project Lab — checkpoint checker for Blinkit Warehouse Pipeline.

Run from the ROOT of your cloned project (the folder with dbt_project.yml):

    python scripts/checkpoint.py <mode>

Modes: snowflake · dataset · raw · dbt · models · tests

It prints a proof block. Copy the WHOLE block (including the ===== lines) and
paste it into the checkpoint box on prepnplaced.com.

SECURITY GUARANTEE — this project involves a Snowflake password, so read this
and then read the code, which is short enough to check line by line:

  * This script NEVER opens profiles.yml. It only asks the operating system
    whether that file exists (Path.exists()). Your password stays on your
    machine, is never printed, and is never pasted into the website.
  * It never reads config/local_credentials.json, .env, or any environment
    variable.
  * It never connects to Snowflake and never sends anything anywhere. It reads
    local files only, prints a block, and exits.
  * From logs/dbt.log it looks for one fixed success phrase and emits only
    true/false. No log content is copied into the output.
"""
from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

ROOT = Path.cwd()
PROJECT = "blinkit-dbt-pipeline"

MARKERS = [
    "dbt_project.yml",
    "models/staging/stg_blinkit_orders.sql",
    "models/marts/fct_blinkit_sales.sql",
    "models/marts/mart_outlet_sales.sql",
]

DATASET = "data/blinkit_orders.csv"

CANONICAL_COLUMNS = [
    "ITEM FAT CONTENT",
    "ITEM IDENTIFIER",
    "ITEM TYPE",
    "OUTLET ESTABLISHMENT YEAR",
    "OUTLET IDENTIFIER",
    "OUTLET LOCATION TYPE",
    "OUTLET SIZE",
    "OUTLET TYPE",
    "ITEM VISIBILITY",
    "ITEM WEIGHT",
    "SALES",
    "RATING",
]

# profiles.yml is referenced ONLY through Path.exists() below. It is never read.
PROFILES = "profiles.yml"


# --------------------------------------------------------------- small helpers


def _fail(msg: str) -> None:
    print(f"\n  ✗ {msg}\n")
    sys.exit(1)


def _require_root() -> None:
    missing = [m for m in MARKERS if not (ROOT / m).exists()]
    if missing:
        _fail(
            "Run this from the project root — the folder that contains "
            f"dbt_project.yml and models/. Missing: {', '.join(missing)}"
        )


def _norm(text: str) -> str:
    """Upper case, single spaces, no quotes — for comparing column names."""
    cleaned = str(text).replace("﻿", "").strip().strip('"').strip("'")
    return re.sub(r"\s+", " ", cleaned).upper()


def _as_bool(value: object) -> bool:
    return str(value).strip().lower() in {"true", "t", "yes", "y", "1"}


def _read_json(relative: str, hint: str) -> dict:
    path = ROOT / relative
    if not path.exists():
        _fail(f"{relative} not found — {hint}")
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            return json.load(handle)
    except json.JSONDecodeError:
        _fail(f"{relative} is not valid JSON — delete target/ and re-run the dbt command.")
    return {}


def _evidence_rows(relative: str, hint: str) -> list[dict]:
    """Read a CSV exported from Snowsight into a list of lower-cased dicts."""
    path = ROOT / relative
    if not path.exists():
        found = sorted(p.name for p in (ROOT / "evidence").glob("*.csv")) if (ROOT / "evidence").is_dir() else []
        extra = f" Files currently in evidence/: {', '.join(found)}" if found else " The evidence/ folder is empty or missing."
        _fail(f"{relative} not found — {hint}.{extra}")
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [
            {(k or "").strip().strip('"').lower(): (v or "").strip().strip('"') for k, v in row.items()}
            for row in reader
        ]
    if not rows:
        _fail(f"{relative} has a header but no rows — re-run the query and download the result again.")
    return rows


def _pick(row: dict, *names: str) -> str:
    for name in names:
        if name in row and str(row[name]).strip() != "":
            return str(row[name]).strip()
    return ""


def _sniff_delimiter(first_line: str) -> str:
    counts = {d: first_line.count(d) for d in (",", ";", "\t", "|")}
    best = max(counts, key=lambda d: counts[d])
    return best if counts[best] > 0 else ","


def _dataset_shape() -> tuple[str, list[str], int]:
    """Return (delimiter, raw header names, data row count) for the local CSV."""
    path = ROOT / DATASET
    if not path.exists():
        found = sorted(p.name for p in (ROOT / "data").glob("*.csv")) if (ROOT / "data").is_dir() else []
        extra = f" CSV files found in data/: {', '.join(found)}" if found else " The data/ folder is empty or missing."
        _fail(f"{DATASET} not found — finish Module 1 Lesson 3 first.{extra}")
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        first_line = handle.readline()
        delimiter = _sniff_delimiter(first_line)
        handle.seek(0)
        reader = csv.reader(handle, delimiter=delimiter)
        try:
            header = next(reader)
        except StopIteration:
            _fail(f"{DATASET} is empty.")
            header = []
        rows = sum(1 for row in reader if any(str(cell).strip() for cell in row))
    return delimiter, header, rows


# ---------------------------------------------------------------------- modes


def mode_snowflake() -> dict:
    _require_root()
    warehouse = _evidence_rows(
        "evidence/warehouse_check.csv",
        "export the SHOW WAREHOUSES / RESULT_SCAN query from the checkpoint card and save it there",
    )[0]
    schemas = _evidence_rows(
        "evidence/schemas_check.csv",
        "export the INFORMATION_SCHEMA.SCHEMATA query from the checkpoint card and save it there",
    )

    size_raw = _pick(warehouse, "warehouse_size", "size")
    suspend_raw = _pick(warehouse, "auto_suspend_seconds", "auto_suspend")
    try:
        auto_suspend = int(float(suspend_raw)) if suspend_raw else -1
    except ValueError:
        auto_suspend = -1

    schema_names = sorted({_norm(_pick(row, "schema_name", "name")) for row in schemas if _pick(row, "schema_name", "name")})
    databases = {_norm(_pick(row, "database_name", "catalog_name")) for row in schemas if _pick(row, "database_name", "catalog_name")}

    return {
        "repo": PROJECT,
        "repo_files_ok": True,
        "warehouse_name": _pick(warehouse, "warehouse_name", "name").upper(),
        "warehouse_size": re.sub(r"[^A-Z0-9]", "", size_raw.upper()),
        "auto_suspend_seconds": auto_suspend,
        "auto_resume": _as_bool(_pick(warehouse, "auto_resume")),
        "database_name": sorted(databases)[0] if databases else "",
        "schemas_found": schema_names,
        "credentials_read": False,
    }


def mode_dataset() -> dict:
    _require_root()
    delimiter, header, row_count = _dataset_shape()
    columns = [_norm(name) for name in header if str(name).strip() != ""]
    return {
        "file": DATASET,
        "delimiter": delimiter,
        "column_count": len(columns),
        "columns": columns,
        "columns_raw": [str(name).strip() for name in header],
        "row_count": row_count,
        "header_ok": sorted(columns) == sorted(CANONICAL_COLUMNS),
        "column_order_ok": columns == CANONICAL_COLUMNS,
        "credentials_read": False,
    }


def mode_raw() -> dict:
    _require_root()
    counts = _evidence_rows(
        "evidence/raw_rowcount.csv",
        "export the COUNT(*) query from the checkpoint card and save it there",
    )[0]
    stages = _evidence_rows(
        "evidence/stage_check.csv",
        "export the SHOW STAGES / RESULT_SCAN query from the checkpoint card and save it there",
    )

    try:
        raw_rows = int(float(_pick(counts, "row_count", "raw_rows_loaded", "count(*)") or -1))
    except ValueError:
        raw_rows = -1

    stage = next((s for s in stages if _norm(_pick(s, "stage_name", "name")) == "BLINKIT_STAGE"), stages[0])
    stage_type = _norm(_pick(stage, "stage_type", "type"))
    if stage_type.startswith("INTERNAL"):
        stage_type = "INTERNAL"
    elif stage_type.startswith("EXTERNAL"):
        stage_type = "EXTERNAL"

    _, _, csv_rows = _dataset_shape()

    return {
        "raw_table": _norm(_pick(counts, "table_name") or "BLINKIT_DB.RAW.BLINKIT_ORDERS_RAW"),
        "raw_row_count": raw_rows,
        "csv_row_count": csv_rows,
        "row_counts_match": raw_rows == csv_rows,
        "stage_name": _norm(_pick(stage, "stage_name", "name")),
        "stage_type": stage_type,
        "credentials_read": False,
    }


def _project_name_from_yml() -> str:
    path = ROOT / "dbt_project.yml"
    if not path.exists():
        return ""
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = re.match(r"^name:\s*['\"]?([A-Za-z0-9_]+)", line)
        if match:
            return match.group(1)
    return ""


def mode_dbt() -> dict:
    _require_root()
    manifest = _read_json(
        "target/manifest.json",
        "run `dbt compile --profiles-dir .` from the project root first.",
    )
    metadata = manifest.get("metadata") or {}

    sources = sorted(
        f"{node.get('source_name')}.{node.get('name')}"
        for node in (manifest.get("sources") or {}).values()
    )

    connection_ok = False
    log_path = ROOT / "logs" / "dbt.log"
    if log_path.exists():
        with open(log_path, "rb") as handle:
            handle.seek(0, 2)
            handle.seek(max(0, handle.tell() - 400_000))
            text = handle.read().decode("utf-8", errors="replace")
        connection_ok = ("Connection test: [OK connection ok]" in text) or ("All checks passed!" in text)

    return {
        # Existence only. This script never opens profiles.yml.
        "profiles_yml_present": (ROOT / PROFILES).exists(),
        "profiles_yml_read": False,
        "credentials_read": False,
        "adapter_type": str(metadata.get("adapter_type") or ""),
        "dbt_version": str(metadata.get("dbt_version") or ""),
        "project_name": str(metadata.get("project_name") or _project_name_from_yml()),
        "sources_declared": sources,
        "debug_connection_ok": connection_ok,
    }


def _model_nodes(manifest: dict) -> dict:
    return {
        node.get("name"): node
        for node in (manifest.get("nodes") or {}).values()
        if node.get("resource_type") == "model" and node.get("name")
    }


def mode_models() -> dict:
    _require_root()
    manifest = _read_json(
        "target/manifest.json",
        "run `dbt build --profiles-dir .` from the project root first.",
    )
    results = _read_json(
        "target/run_results.json",
        "run `dbt build --profiles-dir .` — `dbt compile` alone does not create it.",
    )

    models = _model_nodes(manifest)
    materializations = {}
    for name, node in models.items():
        config = node.get("config") or {}
        schema = node.get("schema") or config.get("schema") or ""
        materializations[name] = {
            "materialized": str(config.get("materialized") or "").lower(),
            "schema": str(schema).lower(),
        }

    succeeded = sorted(
        str(entry.get("unique_id", "")).split(".")[-1]
        for entry in (results.get("results") or [])
        if str(entry.get("unique_id", "")).startswith("model.")
        and str(entry.get("status", "")).lower() == "success"
    )

    return {
        "run_results_present": True,
        "model_count": len(models),
        "models_declared": sorted(models),
        "models_built": succeeded,
        "models_succeeded": len(succeeded),
        "materializations": materializations,
        "credentials_read": False,
    }


def mode_tests() -> dict:
    _require_root()
    results = _read_json(
        "target/run_results.json",
        "run `dbt docs generate --profiles-dir .` FIRST, then `dbt build --profiles-dir .`.",
    )

    entries = [
        entry
        for entry in (results.get("results") or [])
        if str(entry.get("unique_id", "")).startswith("test.")
    ]

    def _count(*wanted: str) -> int:
        return sum(1 for e in entries if str(e.get("status", "")).lower() in wanted)

    names = sorted(
        {
            parts[2]
            for e in entries
            if len(parts := str(e.get("unique_id", "")).split(".")) > 2
        }
    )

    catalog_path = ROOT / "target" / "catalog.json"
    catalog_nodes = 0
    if catalog_path.exists():
        try:
            with open(catalog_path, "r", encoding="utf-8", errors="replace") as handle:
                catalog = json.load(handle)
            catalog_nodes = len(catalog.get("nodes") or {})
        except json.JSONDecodeError:
            catalog_nodes = 0

    return {
        "run_results_present": True,
        "tests_run": len(entries),
        "tests_passed": _count("pass"),
        "tests_failed": _count("fail"),
        "tests_errored": _count("error"),
        "tests_warned": _count("warn"),
        "test_names": names,
        "catalog_present": catalog_path.exists(),
        "catalog_nodes": catalog_nodes,
        "credentials_read": False,
    }


MODES = {
    "snowflake": mode_snowflake,
    "dataset": mode_dataset,
    "raw": mode_raw,
    "dbt": mode_dbt,
    "models": mode_models,
    "tests": mode_tests,
}

# Keys where the healthy value is 0 or False, so a plain truthiness tick would lie.
ZERO_IS_GOOD = {"tests_failed", "tests_errored", "tests_warned", "credentials_read", "profiles_yml_read"}


def _tick(key: str, value: object) -> str:
    if key in ZERO_IS_GOOD:
        good = not bool(value)
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        good = value > 0
    else:
        good = bool(value)
    return "✓" if good else "✗"


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in MODES:
        print(__doc__)
        sys.exit(2)
    block = MODES[sys.argv[1]]()
    payload = {"project": PROJECT, "mode": sys.argv[1], **block}
    print("===== PREPNPLACED CHECKPOINT =====")
    print(json.dumps(payload, indent=2, sort_keys=False))
    print("===== END =====")
    print("\n".join(f"  {_tick(k, v)} {k}: {v}" for k, v in block.items()))
    print("\nCopy everything between the ===== lines into the checkpoint box on the site.")
    print("Your Snowflake password is not in that block, and this script never read it.")


if __name__ == "__main__":
    main()
