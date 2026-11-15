#!/usr/bin/env python3
"""PrepNPlaced Project Lab — checkpoint checker for Retail Revenue Intelligence.

Run from the ROOT of your cloned project:

    python scripts/checkpoint.py <mode>

Modes: env · data · clean · sql · dashboard

It prints a proof block. Copy the WHOLE block (including the ===== lines) and
paste it into the checkpoint box on prepnplaced.com. The script only reads your
project folder and localhost:8501 — it never sends anything anywhere itself.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path.cwd()

# Windows sends redirected output through cp1252, which has no ✓ or ✗: print UTF-8 everywhere instead.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except (AttributeError, ValueError):
        pass
MARKERS = ["src/download_data.py", "dashboard/app.py"]


def _fail(msg: str) -> None:
    print(f"\n  ✗ {msg}\n")
    sys.exit(1)


def _mark(value: object) -> str:
    """✗ only for something missing. A count of 0 is a real answer (no nulls left is exactly what a clean
    table should show), so it gets a tick; -1 means the column was not found."""
    missing = value is None or value is False or value == -1 or value in ("", [], {})
    return "✗" if missing else "✓"


def _sha256_prefix(path: Path, chars: int = 16) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()[:chars]


def _first_existing(*candidates: str) -> Path | None:
    for candidate in candidates:
        path = ROOT / candidate
        if path.exists():
            return path
    return None


def mode_env() -> dict:
    if not all((ROOT / marker).exists() for marker in MARKERS):
        _fail("Run this from the project root — the folder that contains src/ and dashboard/.")
    git_ok = False
    try:
        name = subprocess.run(["git", "config", "user.name"], capture_output=True, text=True).stdout.strip()
        email = subprocess.run(["git", "config", "user.email"], capture_output=True, text=True).stdout.strip()
        git_ok = bool(name and email)
    except OSError:
        pass
    try:
        import pandas  # noqa: F401
        import pyarrow  # noqa: F401
        import streamlit  # noqa: F401
        requirements_ok = True
    except ImportError:
        requirements_ok = False
    in_venv = sys.prefix != getattr(sys, "base_prefix", sys.prefix)
    # A GitHub Codespace or a dev container is already an isolated environment
    # (the container is the clean room), so it counts as much as a venv does.
    if os.environ.get("CODESPACES") == "true":
        environment = "codespace"
    elif os.environ.get("REMOTE_CONTAINERS") == "true" or os.environ.get("DEVCONTAINER") == "true":
        environment = "dev container"
    else:
        environment = "venv" if in_venv else "system python"
    return {
        "python": ".".join(map(str, sys.version_info[:3])),
        "venv_active": environment != "system python",
        "environment": environment,
        "git_configured": git_ok,
        "repo": "retail-revenue-intelligence",
        "requirements_installed": requirements_ok,
    }


def mode_data() -> dict:
    path = _first_existing("data/raw/Online Retail.xlsx")
    if path is None:
        _fail("data/raw/Online Retail.xlsx not found — finish the download lesson first.")
    return {
        "file": "data/raw/Online Retail.xlsx",
        "size_bytes": path.stat().st_size,
        "sha256_prefix": _sha256_prefix(path),
    }


def mode_clean() -> dict:
    parquet = _first_existing("data/processed/retail_orders.parquet")
    sqlite_db = _first_existing("data/processed/retail_analytics.sqlite")
    if parquet is None:
        _fail("data/processed/retail_orders.parquet not found — run the prepare step first.")
    try:
        import pandas as pd
    except ImportError:
        _fail("pandas is not installed in this venv — activate your venv first.")
    frame = pd.read_parquet(parquet)

    def col(*names: str):
        for name in names:
            if name in frame.columns:
                return frame[name]
        lowered = {c.lower().replace(" ", "_"): c for c in frame.columns}
        for name in names:
            hit = lowered.get(name.lower().replace(" ", "_"))
            if hit:
                return frame[hit]
        return None

    invoice = col("InvoiceNo", "invoice_no", "invoice")
    cancelled = int(invoice.astype(str).str.startswith("C").sum()) if invoice is not None else -1
    outputs = []
    if parquet is not None:
        outputs.append("data/processed/retail_orders.parquet")
    if sqlite_db is not None:
        outputs.append("data/processed/retail_analytics.sqlite")

    def nulls(*names: str) -> int:
        series = col(*names)
        return int(series.isna().sum()) if series is not None else -1

    return {
        "row_count": int(len(frame)),
        "null_invoice_date": nulls("InvoiceDate", "invoice_date"),
        "null_quantity": nulls("Quantity", "quantity"),
        "null_unit_price": nulls("UnitPrice", "unit_price"),
        "cancelled_rows": cancelled,
        "outputs": outputs,
    }


def mode_sql() -> dict:
    queries = {}
    for name in ("monthly_revenue", "top_countries", "top_products"):
        # src/sql_analysis.py writes outputs/sql_results/; the other two are older layouts, still accepted.
        path = _first_existing(f"outputs/sql_results/{name}.csv", f"outputs/sql/{name}.csv", f"outputs/{name}.csv")
        if path is None:
            _fail(f"{name}.csv not found under outputs/ — run the SQL analysis step first.")
        rows = max(0, sum(1 for _ in open(path, "r", encoding="utf-8", errors="replace")) - 1)
        queries[name] = {"row_count": rows, "csv_sha256_prefix": _sha256_prefix(path)}
    return {"queries": queries}


def mode_dashboard() -> dict:
    import urllib.request

    try:
        with urllib.request.urlopen("http://localhost:8501", timeout=5) as response:
            html = response.read(65536).decode("utf-8", errors="replace")
    except OSError:
        _fail("Nothing is answering on http://localhost:8501 — is `streamlit run dashboard/app.py` running in another terminal?")
    match = re.search(r"<title>([^<]*)</title>", html)
    title = (match.group(1).strip() if match else "")
    if "Retail" not in title:
        # Streamlit sets the real title after JS boot; fall back to the page config in source.
        source = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8", errors="replace")
        config = re.search(r"page_title\s*=\s*[\"']([^\"']+)[\"']", source)
        if config and "Retail" in config.group(1):
            title = config.group(1).strip()
    return {"port": 8501, "page_title": title}


MODES = {"env": mode_env, "data": mode_data, "clean": mode_clean, "sql": mode_sql, "dashboard": mode_dashboard}


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in MODES:
        print(__doc__)
        sys.exit(2)
    block = MODES[sys.argv[1]]()
    payload = {"project": "retail-revenue-intelligence", "mode": sys.argv[1], **block}
    print("===== PREPNPLACED CHECKPOINT =====")
    print(json.dumps(payload, indent=2))
    print("===== END =====")
    checks = [f"  {_mark(v)} {k}: {v}" for k, v in block.items()]
    print("\n".join(checks))
    print("\nCopy everything between the ===== lines into the checkpoint box on the site.")


if __name__ == "__main__":
    main()
