#!/usr/bin/env python3
"""PrepNPlaced Project Lab: the checker for Airbnb Analytics Engineering (dbt).

It runs in two places.

1. On your laptop or in your Codespace, for the two setup checkpoints. From
   the project folder (the one with dbt_project.yml):

       python lab_check.py env     your tools are ready and you work in your own fork
       python lab_check.py raw     the four RAW tables are loaded into airbnb.duckdb

   Each prints a proof block between ===== lines. Paste the whole block into the
   checkpoint box on prepnplaced.com.

2. On GitHub, inside your repository's "PrepNPlaced Lab check" workflow, for the
   build checkpoints (you can run them locally too, before you push):

       python lab_check.py ci staging        the m2 assignment
       python lab_check.py ci intermediate   the m3 assignment
       python lab_check.py ci marts          the m4 assignment
       python lab_check.py ci build-it       m5: the mart and the incremental model you write
       python lab_check.py ci break-it       m6: the three incidents, and your notes
       python lab_check.py ci ship           m7: your change, your README, everything above green

   Each prints a line per check and exits non-zero when one fails. The Lab reads
   the result of the matching step from GitHub; nothing is pasted.

3. On your machine, to practise an incident before you fix it (Modules 4 and 6):

       python lab_check.py drill new-snapshot      50 listings disappear from the calendar
       python lab_check.py drill duplicate-days    the calendar export repeats some days
       python lab_check.py drill european-prices   a third of prices arrive as $1.234,50
       python lab_check.py drill late-reviews      15 reviews dated June 2015 arrive late
       python lab_check.py drill reset             reload RAW from data/raw (undo any drill)

   The GitHub check applies the very same statements to throwaway copies.

What it reads: your project files, dbt's target/ artifacts and airbnb.duckdb.
To test YOUR tests (does your new test really catch a broken row?) the ci checks
copy the database into a temporary folder, break the copy on purpose and run your
dbt project against the copy, never against your own files. Only `drill` changes
your local airbnb.duckdb, and only when you run it. It never opens profiles.yml,
never reads credentials, and never sends anything anywhere.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path.cwd()
PROJECT = "airbnb-snowflake-dbt"
DATABASE = ROOT / "airbnb.duckdb"
TARGET = ROOT / "target"

UPSTREAM_MODELS = {
    "stg_airbnb__listings", "stg_airbnb__calendar", "stg_airbnb__reviews", "stg_airbnb__neighbourhoods",
    "int_airbnb__listing_enriched", "int_airbnb__calendar_enriched", "int_airbnb__reviews_enriched",
    "dim_listings", "dim_hosts", "fct_listing_calendar", "fct_reviews",
    "agg_listing_monthly_performance", "agg_neighbourhood_monthly_performance",
}
UPSTREAM_SINGULAR_TESTS = {"assert_no_duplicate_listing_calendar", "assert_non_negative_prices"}
STAGING = ["stg_airbnb__listings", "stg_airbnb__calendar", "stg_airbnb__reviews", "stg_airbnb__neighbourhoods"]
INTERMEDIATE = ["int_airbnb__listing_enriched", "int_airbnb__calendar_enriched", "int_airbnb__reviews_enriched"]
MARTS = ["dim_listings", "dim_hosts", "fct_listing_calendar", "fct_reviews",
         "agg_listing_monthly_performance", "agg_neighbourhood_monthly_performance"]
HOST_MART = "agg_host_monthly_performance"
HOST_MART_COLUMNS = ["host_id", "calendar_month", "listing_count", "available_nights",
                     "unavailable_nights", "availability_rate", "estimated_revenue"]
# Reviews the checks plant use 20-digit ids: Airbnb ids are 64-bit (at most 19 digits), so a planted
# review can never collide with a real one. Staging casts ids to NUMBER(38, 0), which holds them.
FUTURE_REVIEW_ID = 90000000000000000001
NEW_REVIEW_FIRST_ID = 91000000000000000000
LATE_REVIEW_FIRST_ID = 92000000000000000000


# ---------------------------------------------------------------- reporting

class Report:
    def __init__(self, title: str):
        self.title = title
        self.lines: list[tuple[bool, str]] = []

    def check(self, ok: bool, message: str) -> bool:
        self.lines.append((bool(ok), message))
        print(f"  {'✓' if ok else '✗'} {message}", flush=True)
        return bool(ok)

    @property
    def ok(self) -> bool:
        return bool(self.lines) and all(ok for ok, _ in self.lines)


# ---------------------------------------------------------------- dbt + duckdb

def duckdb_module():
    try:
        import duckdb  # noqa: PLC0415
    except ImportError:
        print("  ✗ duckdb is not installed here. Run: pip install -r requirements-duckdb.txt")
        sys.exit(2)
    return duckdb


def query(sql: str, database: Path = DATABASE) -> list[tuple]:
    duckdb = duckdb_module()
    with duckdb.connect(str(database), read_only=True) as connection:
        return connection.execute(sql).fetchall()


def scalar(sql: str, database: Path = DATABASE):
    rows = query(sql, database)
    return rows[0][0] if rows else None


def relation_exists(schema: str, name: str, database: Path = DATABASE) -> bool:
    return bool(scalar(
        "select count(*) from (select schema_name s, table_name n from duckdb_tables() "
        "union all select schema_name, view_name from duckdb_views()) "
        f"where lower(s) = '{schema}' and lower(n) = '{name}'", database))


def columns_of(schema: str, name: str, database: Path = DATABASE) -> list[str]:
    return [row[0].lower() for row in query(f"describe {schema}.{name}", database)]


def load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def manifest(target: Path = TARGET) -> dict:
    return load_json(target / "manifest.json")


def run_results(target: Path = TARGET) -> dict[str, str]:
    """unique_id -> status ('success', 'pass', 'fail', 'error', 'warn', 'skipped')."""
    return {r.get("unique_id"): r.get("status") for r in load_json(target / "run_results.json").get("results", [])}


def model_nodes(man: dict) -> dict[str, dict]:
    return {n["name"]: n for n in man.get("nodes", {}).values() if n.get("resource_type") == "model"}


def tests_on(man: dict, model_name: str) -> list[dict]:
    """Every data test that depends on the model."""
    model_ids = {uid for uid, n in man.get("nodes", {}).items() if n.get("resource_type") == "model" and n.get("name") == model_name}
    return [n for n in man.get("nodes", {}).values()
            if n.get("resource_type") == "test" and model_ids & set((n.get("depends_on") or {}).get("nodes") or [])]


def generic_test(man: dict, model_name: str, test_name: str, column: str) -> list[dict]:
    out = []
    for node in tests_on(man, model_name):
        meta = node.get("test_metadata") or {}
        kwargs = meta.get("kwargs") or {}
        if meta.get("name") == test_name and str(kwargs.get("column_name", "")).lower() == column:
            out.append(node)
    return out


def kind_of(result: dict) -> str:
    """'model', 'test', 'seed', ... from a run_results entry: unique_ids look like 'test.project.name'."""
    return str(result.get("unique_id") or "").split(".", 1)[0]


def dbt_cli() -> list[str]:
    exe = shutil.which("dbt")
    return [exe] if exe else [sys.executable, "-m", "dbt.cli.main"]


def profile_name() -> str:
    text = (ROOT / "dbt_project.yml").read_text(encoding="utf-8") if (ROOT / "dbt_project.yml").exists() else ""
    match = re.search(r"^profile:\s*['\"]?([\w-]+)", text, re.M)
    return match.group(1) if match else "airbnb_snowflake"


class Sandbox:
    """A throwaway copy of airbnb.duckdb with its own profile, target and logs.

    Breaking the copy and running your dbt project against it shows what your
    models and tests do with bad data, without touching your own files.
    """

    def __init__(self):
        self.folder = Path(tempfile.mkdtemp(prefix="prepnplaced-lab-"))
        self.database = self.folder / "airbnb.duckdb"
        self.target = self.folder / "target"
        shutil.copy2(DATABASE, self.database)
        wal = DATABASE.with_name(DATABASE.name + ".wal")
        if wal.exists():
            shutil.copy2(wal, self.folder / wal.name)
        (self.folder / "profiles.yml").write_text(
            f"{profile_name()}:\n  target: duckdb\n  outputs:\n    duckdb:\n      type: duckdb\n"
            f"      path: \"{self.database.as_posix()}\"\n      schema: analytics\n      threads: 4\n",
            encoding="utf-8",
        )

    def execute(self, *statements: str) -> None:
        duckdb = duckdb_module()
        with duckdb.connect(str(self.database)) as connection:
            for statement in statements:
                connection.execute(statement)

    def query(self, sql: str) -> list[tuple]:
        return query(sql, self.database)

    def scalar(self, sql: str):
        return scalar(sql, self.database)

    def dbt(self, *args: str) -> tuple[int, str]:
        command = [*dbt_cli(), *args, "--profiles-dir", str(self.folder), "--target", "duckdb",
                   "--target-path", str(self.target), "--log-path", str(self.folder / "logs")]
        done = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=900)
        return done.returncode, (done.stdout or "") + (done.stderr or "")

    def failed_tests(self) -> list[str]:
        results = load_json(self.target / "run_results.json").get("results", [])
        return [r.get("unique_id", "") for r in results if r.get("status") in ("fail", "error") and kind_of(r) == "test"]

    def errored_models(self) -> list[str]:
        results = load_json(self.target / "run_results.json").get("results", [])
        return [r.get("unique_id", "") for r in results if r.get("status") == "error" and kind_of(r) == "model"]

    def close(self) -> None:
        shutil.rmtree(self.folder, ignore_errors=True)


def need_build(report: Report) -> dict:
    """The CI build (or your local dbt build --target duckdb) must have happened first."""
    man = manifest()
    report.check(DATABASE.exists(), "airbnb.duckdb exists (python scripts/load_inside_airbnb_to_duckdb.py, then dbt build --target duckdb --profiles-dir .)")
    report.check(bool(man) and bool(load_json(TARGET / "run_results.json")), "dbt build has run (target/manifest.json and target/run_results.json exist)")
    return man


def models_built(report: Report, man: dict, names: list[str]) -> None:
    results = run_results()
    nodes = model_nodes(man)
    for name in names:
        node = nodes.get(name)
        status = results.get(node["unique_id"]) if node else None
        report.check(status == "success", f"{name} built" + ("" if status == "success" else f" (status: {status or 'not run'})"))


def tests_pass(report: Report, man: dict, names: list[str]) -> None:
    results = run_results()
    bad, total = [], 0
    for name in names:
        for node in tests_on(man, name):
            total += 1
            if results.get(node["unique_id"]) not in ("pass", "warn"):
                bad.append(node["name"])
    report.check(not bad, f"{total} tests on these models pass" + (f"; failing or not run: {sorted(set(bad))[:6]}" if bad else ""))


# ---------------------------------------------------------------- local proof modes

def git_origin() -> str:
    try:
        done = subprocess.run(["git", "config", "--get", "remote.origin.url"], cwd=ROOT, capture_output=True, text=True, timeout=10)
        return done.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def mode_env() -> dict:
    import platform  # noqa: PLC0415
    block: dict = {"python": platform.python_version()}
    try:
        done = subprocess.run([*dbt_cli(), "--version"], capture_output=True, text=True, timeout=120)
        text = done.stdout + done.stderr
        core = re.search(r"installed:\s*([0-9][\w.\-]*)", text)
        block["dbt"] = core.group(1) if core else "not found"
        block["duckdb_adapter"] = bool(re.search(r"-\s*duckdb:\s*[0-9]", text))
    except (OSError, subprocess.SubprocessError):
        block["dbt"], block["duckdb_adapter"] = "not found", False
    block["dbt_project"] = "airbnb_snowflake_dbt" if "name: airbnb_snowflake_dbt" in (ROOT / "dbt_project.yml").read_text(encoding="utf-8") else "wrong folder"
    block["profiles_file"] = (ROOT / "profiles.yml").exists()
    origin = git_origin()
    owner = re.search(r"github\.com[:/]([^/]+)/", origin)
    block["origin_owner_is_you"] = bool(owner) and owner.group(1).lower() != "analyticsdurgesh"
    return block


def mode_raw() -> dict:
    if not DATABASE.exists():
        return {"database": False}
    block: dict = {"database": True}
    for table in ("listings", "calendar", "reviews", "neighbourhoods"):
        block[f"raw_{table}"] = int(scalar(f"select count(*) from raw.{table}") or 0) if relation_exists("raw", table) else 0
    types = {row[0] for row in query("select data_type from information_schema.columns where table_schema = 'raw'")}
    block["raw_all_text"] = types == {"VARCHAR"}
    return block


# ---------------------------------------------------------------- CI checkpoints

def ci_staging(report: Report) -> None:
    man = need_build(report)
    if not report.ok:
        return
    models_built(report, man, STAGING)
    tests_pass(report, man, STAGING)
    report.check(bool(generic_test(man, "stg_airbnb__reviews", "not_null", "review_date")),
                 "assignment 1: a not_null test on stg_airbnb__reviews.review_date")
    singular = [n for n in tests_on(man, "stg_airbnb__reviews")
                if not n.get("test_metadata") and n.get("name") not in UPSTREAM_SINGULAR_TESTS]
    if not report.check(bool(singular), "assignment 2: your own singular test (tests/*.sql) on stg_airbnb__reviews"):
        return
    box = Sandbox()
    try:
        listing = box.scalar("select listing_id from raw.reviews limit 1")
        box.execute(f"insert into raw.reviews (listing_id, id, date, reviewer_id, reviewer_name, comments) "
                    f"values ('{listing}', '{FUTURE_REVIEW_ID}', '2099-01-01', '1', 'Time Traveller', 'Lovely, will stay again in 2099')")
        box.dbt("run", "--select", "stg_airbnb__reviews")
        code, _ = box.dbt("test", "--select", "stg_airbnb__reviews")
        failed = box.failed_tests()
        caught = code != 0 and any(n["unique_id"] in failed for n in singular)
        report.check(caught, "assignment 2: with a review dated 2099 planted in RAW, your singular test fails"
                     + ("" if caught else f" (tests that failed: {[uid.split('.')[-1] for uid in failed] or 'none'})"))
    finally:
        box.close()


def ci_intermediate(report: Report) -> None:
    man = need_build(report)
    if not report.ok:
        return
    models_built(report, man, INTERMEDIATE)
    tests_pass(report, man, INTERMEDIATE)
    cols = columns_of("intermediate", "int_airbnb__listing_enriched") if relation_exists("intermediate", "int_airbnb__listing_enriched") else []
    if not report.check("is_long_stay" in cols, "assignment: int_airbnb__listing_enriched has an is_long_stay column"):
        return
    wrong = scalar("""
        select count(*) from intermediate.int_airbnb__listing_enriched
        where is_long_stay is distinct from (case when minimum_nights is null then null else minimum_nights >= 30 end)
    """)
    report.check(wrong == 0, f"assignment: is_long_stay is true exactly when minimum_nights >= 30 (NULL when unknown); rows that disagree: {wrong}")
    tested = [n for n in tests_on(man, "int_airbnb__listing_enriched")
              if str(((n.get("test_metadata") or {}).get("kwargs") or {}).get("column_name", "")).lower() == "is_long_stay"
              or "is_long_stay" in (n.get("raw_code") or "").lower()]
    report.check(bool(tested), "assignment: at least one test covers is_long_stay")


def ci_marts(report: Report) -> None:
    man = need_build(report)
    if not report.ok:
        return
    models_built(report, man, MARTS)
    tests_pass(report, man, MARTS)
    raw_calendar = scalar("select count(*) from raw.calendar")
    fact = scalar("select count(*) from analytics.fct_listing_calendar")
    report.check(fact == raw_calendar, f"fct_listing_calendar has one row per raw calendar row ({fact:,} vs {raw_calendar:,})")
    revenue = scalar("select coalesce(sum(estimated_unavailable_revenue), 0) from analytics.agg_neighbourhood_monthly_performance") or 0
    report.check(revenue > 0, f"estimated revenue is positive (${revenue:,.0f})")
    rel_listing = [n for n in generic_test(man, "fct_listing_calendar", "relationships", "listing_id")
                   if "dim_listings" in json.dumps((n.get("test_metadata") or {}).get("kwargs") or {})]
    rel_host = [n for n in generic_test(man, "dim_listings", "relationships", "host_id")
                if "dim_hosts" in json.dumps((n.get("test_metadata") or {}).get("kwargs") or {})]
    report.check(bool(rel_listing), "assignment: a relationships test from fct_listing_calendar.listing_id to dim_listings")
    report.check(bool(rel_host), "assignment: a relationships test from dim_listings.host_id to dim_hosts")
    if not (rel_listing and rel_host):
        return
    box = Sandbox()
    try:
        box.execute("delete from analytics.dim_listings where listing_id = (select min(listing_id) from analytics.fct_listing_calendar)")
        code, _ = box.dbt("test", "--select", "fct_listing_calendar")
        caught = code != 0 and any(n["unique_id"] in box.failed_tests() for n in rel_listing)
        report.check(caught, "with a listing deleted from dim_listings, your relationships test catches the orphaned calendar rows")
    finally:
        box.close()


def host_mart_reference(database: Path) -> dict[tuple, tuple]:
    rows = query("""
        select host_id, calendar_month,
               count(distinct listing_id),
               count(*) filter (where is_available),
               count(*) filter (where not is_available),
               round(count(*) filter (where is_available) / count(*), 4),
               round(sum(estimated_unavailable_night_revenue), 2)
        from analytics.fct_listing_calendar
        group by host_id, calendar_month
    """, database)
    return {(r[0], r[1]): tuple(r[2:]) for r in rows}


def ci_build_it(report: Report) -> None:
    man = need_build(report)
    if not report.ok:
        return
    nodes = model_nodes(man)
    # (a) the mart you write from the spec
    node = nodes.get(HOST_MART)
    results = run_results()
    report.check(bool(node) and results.get(node["unique_id"]) == "success", f"{HOST_MART} built")
    if node and relation_exists("analytics", HOST_MART):
        cols = columns_of("analytics", HOST_MART)
        missing = [c for c in HOST_MART_COLUMNS if c not in cols]
        report.check(not missing, f"{HOST_MART} has the spec's columns" + (f"; missing {missing}" if missing else ""))
        if not missing:
            expected = host_mart_reference(DATABASE)
            got = {(r[0], r[1]): tuple(r[2:]) for r in query(
                f"select {', '.join(HOST_MART_COLUMNS)} from analytics.{HOST_MART}")}
            report.check(len(got) == len(expected), f"{HOST_MART} has one row per host per month ({len(got)} vs {len(expected)} expected)")
            off = 0
            for key, want in expected.items():
                have = got.get(key)
                if have is None or any(abs(float(h or 0) - float(w or 0)) > (0.0001 if i == 3 else 0.011)
                                       for i, (h, w) in enumerate(zip(have, want))):
                    off += 1
            report.check(off == 0, f"{HOST_MART} values match the spec for every host and month (rows that differ: {off})")
        mart_tests = tests_on(man, HOST_MART)
        if report.check(len(mart_tests) >= 2, f"{HOST_MART} has at least two tests ({len(mart_tests)})"):
            box = Sandbox()
            try:
                box.execute(f"insert into analytics.{HOST_MART} select * from analytics.{HOST_MART} limit 1")
                code, _ = box.dbt("test", "--select", HOST_MART)
                report.check(code != 0 and bool(box.failed_tests()),
                             f"with one host-month duplicated, a test on {HOST_MART} fails (it guards the grain)")
            finally:
                box.close()
    # (b) fct_reviews becomes incremental
    reviews = nodes.get("fct_reviews") or {}
    config = reviews.get("config") or {}
    unique_key = config.get("unique_key")
    keys = [unique_key] if isinstance(unique_key, str) else list(unique_key or [])
    report.check(config.get("materialized") == "incremental", "fct_reviews is materialized as incremental")
    report.check([k.lower() for k in keys] == ["review_id"],
                 "fct_reviews has unique_key review_id" + ("" if [k.lower() for k in keys] == ["review_id"]
                                                          else f" (yours: {', '.join(keys) or 'not set yet'})"))
    if config.get("materialized") != "incremental":
        return
    box = Sandbox()
    try:
        before = box.scalar("select count(*) from analytics.fct_reviews")
        latest = box.scalar("select max(review_date) from analytics.fct_reviews")
        oldest = box.scalar("select review_id from analytics.fct_reviews order by review_date, review_id limit 1")
        # A marker on the OLDEST existing row: an incremental run leaves it alone,
        # a full rebuild recreates the row from source and wipes the marker.
        box.execute(f"update analytics.fct_reviews set reviewer_name = 'PREPNPLACED-MARKER' where review_id = {oldest}")
        listing = box.scalar("select listing_id from raw.reviews limit 1")
        rows = ", ".join(f"('{listing}', '{NEW_REVIEW_FIRST_ID + i}', cast(date '{latest}' + interval 1 day as varchar), '{i}', 'New Guest {i}', 'Arrived today')" for i in range(25))
        box.execute(f"insert into raw.reviews (listing_id, id, date, reviewer_id, reviewer_name, comments) values {rows}")
        code, _ = box.dbt("build", "--select", "stg_airbnb__reviews+", "--exclude", "fct_listing_calendar+")
        after = box.scalar("select count(*) from analytics.fct_reviews")
        dupes = box.scalar("select count(*) - count(distinct review_id) from analytics.fct_reviews")
        marker = box.scalar(f"select reviewer_name from analytics.fct_reviews where review_id = {oldest}")
        report.check(code == 0 and after == before + 25 and dupes == 0,
                     f"25 new reviews arrive: the incremental run adds exactly 25 ({before:,} -> {after:,}, duplicates {dupes})")
        report.check(marker == "PREPNPLACED-MARKER",
                     "the incremental run leaves existing rows alone (it did not rebuild the whole table)")
    finally:
        box.close()


def european(price_column: str) -> str:
    """'$1,234.50' -> '$1.234,50': the incident's export format, as SQL."""
    return (f"case when {price_column} is null then null else "
            f"replace(replace(replace({price_column}, ',', '#'), '.', ','), '#', '.') end")


# The incidents, as SQL. The drills (Modules 4 and 6) apply them to YOUR airbnb.duckdb; the GitHub
# check applies the same statements to throwaway copies, so what you practise is what is checked.
DUPLICATE_DAYS_SQL = "insert into raw.calendar select * from raw.calendar using sample 3 percent (bernoulli, 7)"
EUROPEAN_PRICES_SQL = f"update raw.listings set price = {european('price')} where hash(id) % 3 = 0"
NEW_SNAPSHOT_SQL = """
    delete from raw.calendar where listing_id in (
        select listing_id from (select distinct listing_id from raw.calendar order by listing_id limit 50)
    )"""


def late_reviews_sql(listing_id: str) -> str:
    rows = ", ".join(
        f"('{listing_id}', '{LATE_REVIEW_FIRST_ID + i}', '2015-06-0{1 + i % 9}', '{i}', 'Late Guest {i}', 'Posted late')"
        for i in range(15)
    )
    return f"insert into raw.reviews (listing_id, id, date, reviewer_id, reviewer_name, comments) values {rows}"


def ci_break_it(report: Report) -> None:
    man = need_build(report)
    if not report.ok:
        return
    # Incident 1: the calendar export sends some days twice.
    box = Sandbox()
    try:
        box.execute(DUPLICATE_DAYS_SQL)
        distinct_days = box.scalar("select count(*) from (select distinct listing_id, date from raw.calendar)")
        code, _ = box.dbt("build", "--full-refresh")
        fact = box.scalar("select count(*) from analytics.fct_listing_calendar") if code == 0 else None
        seen = "the build failed with the duplicates in RAW" if fact is None else f"{fact:,} rows vs {distinct_days:,} listing-days"
        report.check(code == 0 and fact == distinct_days,
                     f"incident 1 (duplicate calendar days): the build passes and keeps one row per listing-day ({seen})")
    finally:
        box.close()
    # Incident 2: prices arrive as '$1.234,50' for a third of the listings.
    box = Sandbox()
    try:
        baseline = dict(box.query("select listing_id, nightly_price from analytics.dim_listings"))
        box.execute(EUROPEAN_PRICES_SQL)
        code, _ = box.dbt("build", "--full-refresh")
        wrong = 0
        if code == 0:
            for listing_id, price in box.query("select listing_id, nightly_price from analytics.dim_listings"):
                want = baseline.get(listing_id)
                if (want is None) != (price is None) or (want is not None and abs(float(price) - float(want)) > 0.011):
                    wrong += 1
        report.check(code == 0 and wrong == 0,
                     f"incident 2 (prices as $1.234,50): every listing keeps its true price (listings wrong: {wrong if code == 0 else 'build failed'})")
    finally:
        box.close()
    box = Sandbox()
    try:
        box.execute("update raw.listings set price = '$0.50' where id = (select min(id) from raw.listings where price is not null)")
        code, _ = box.dbt("build", "--full-refresh")
        report.check(code != 0 and bool(box.failed_tests()) and not box.errored_models(),
                     "incident 2: a listing priced $0.50 makes one of your tests fail (a price-range test)")
    finally:
        box.close()
    # Incident 3: reviews arrive late, dated long before the newest review.
    reviews = model_nodes(man).get("fct_reviews") or {}
    if report.check((reviews.get("config") or {}).get("materialized") == "incremental",
                    "incident 3 needs fct_reviews to be incremental (module 5)"):
        box = Sandbox()
        try:
            box.execute(late_reviews_sql(box.scalar("select listing_id from raw.reviews limit 1")))
            code, _ = box.dbt("build", "--select", "stg_airbnb__reviews+", "--exclude", "fct_listing_calendar+")
            raw = box.scalar("select count(*) from raw.reviews")
            fact = box.scalar("select count(*) from analytics.fct_reviews")
            report.check(code == 0 and fact == raw,
                         f"incident 3 (15 late reviews dated 2015): the incremental run picks all of them up ({fact:,} vs {raw:,} raw)")
        finally:
            box.close()
    # Your incident notes.
    notes_file = ROOT / "INCIDENTS.md"
    notes = notes_file.read_text(encoding="utf-8") if notes_file.exists() else ""
    words = len(notes.split())
    sections = len(re.findall(r"(?im)^#{1,4}\s*incident\b", notes))
    keywords = all(len(re.findall(rf"(?i)\b{k}", notes)) >= 3 for k in ("symptom", "cause", "fix", "test"))
    report.check(notes_file.exists() and sections >= 3 and keywords and words >= 150,
                 f"INCIDENTS.md: three '## Incident' sections, each with symptom, cause, fix and test ({sections} sections, {words} words)")


def ci_ship(report: Report) -> None:
    man = need_build(report)
    if not report.ok:
        return
    readme = (ROOT / "README.md").read_text(encoding="utf-8") if (ROOT / "README.md").exists() else ""
    section = re.search(r"(?ims)^#{1,3}\s*what i changed\s*$(.*?)(?=^#{1,3}\s|\Z)", readme)
    words = len(section.group(1).split()) if section else 0
    report.check(words >= 60, f"README.md has a '## What I changed' section of 60+ words ({words})")
    nodes = model_nodes(man)
    results = run_results()
    own_models = [n for name, n in nodes.items() if name not in UPSTREAM_MODELS | {HOST_MART}]
    own_ok = [n for n in own_models if results.get(n["unique_id"]) == "success" and tests_on(man, n["name"])]
    snowflake = load_json(ROOT / "proof" / "snowflake" / "run_results.json")
    snowflake_manifest = load_json(ROOT / "proof" / "snowflake" / "manifest.json")
    snowflake_ok = ((snowflake_manifest.get("metadata") or {}).get("adapter_type") == "snowflake"
                    and sum(1 for r in snowflake.get("results", []) if r.get("status") == "success" and kind_of(r) == "model") >= 13
                    and not any(r.get("status") in ("error", "fail") for r in snowflake.get("results", [])))
    other_city = scalar("select count(*) from raw.listings") not in (500,) and scalar("select count(*) from raw.calendar") != 182500
    twist = [label for label, ok in (("a model of your own, built and tested", bool(own_ok)),
                                     ("a Snowflake cloud run (proof/snowflake)", snowflake_ok),
                                     ("another city's data", other_city)) if ok]
    report.check(bool(twist), "your change: a new model of your own with a test, OR another city's data, OR a Snowflake run proof"
                 + (f" (found: {', '.join(twist)})" if twist else ""))
    earlier = {"staging": ci_staging, "intermediate": ci_intermediate, "marts": ci_marts,
               "build-it": ci_build_it, "break-it": ci_break_it}
    # In the workflow, the five earlier steps already ran in this same job; the
    # workflow hands their outcomes over (LAB_EARLIER="staging=success,...").
    # Run by hand, there is no such list, so this step re-runs them itself.
    handed_over = dict(pair.split("=", 1) for pair in os.environ.get("LAB_EARLIER", "").split(",") if "=" in pair)
    if handed_over:
        for name in earlier:
            outcome = handed_over.get(name, "missing")
            report.check(outcome == "success", f"checkpoint {name} passed in this run ({outcome})")
        return
    print("\n  Re-running the earlier checkpoints so the ship step stands on its own:", flush=True)
    for name, function in earlier.items():
        inner = Report(name)
        print(f"  -- {name}", flush=True)
        function(inner)
        report.check(inner.ok, f"checkpoint {name} still passes")


# ---------------------------------------------------------------- drills (Modules 4 and 6)

def drill_connection():
    duckdb = duckdb_module()
    if not DATABASE.exists():
        print("airbnb.duckdb not found. Run: python scripts/load_inside_airbnb_to_duckdb.py")
        sys.exit(2)
    try:
        return duckdb.connect(str(DATABASE))
    except duckdb.IOException as error:
        print(f"Could not open airbnb.duckdb for writing ({error}).")
        print("Another program has it open: stop the dashboard (Ctrl+C) or any Python session using it, then run this again.")
        sys.exit(2)


def count_in(connection, table: str) -> int:
    return connection.execute(f"select count(*) from raw.{table}").fetchone()[0]


def drill_new_snapshot(connection) -> str:
    before = count_in(connection, "calendar")
    connection.execute(NEW_SNAPSHOT_SQL)
    after = count_in(connection, "calendar")
    return (f"A new snapshot in which 50 listings have gone: RAW.CALENDAR {before:,} -> {after:,} rows.\n"
            "Next: dbt build --target duckdb --profiles-dir .  then  python scripts/verify_duckdb_build.py")


def drill_duplicate_days(connection) -> str:
    before = count_in(connection, "calendar")
    connection.execute(DUPLICATE_DAYS_SQL)
    after = count_in(connection, "calendar")
    return (f"The calendar export repeated {after - before:,} listing-days: RAW.CALENDAR {before:,} -> {after:,} rows.\n"
            "Next: dbt build --target duckdb --profiles-dir . --full-refresh  and read what fails.")


def drill_european_prices(connection) -> str:
    changed = connection.execute("select count(*) from raw.listings where price is not null and hash(id) % 3 = 0").fetchone()[0]
    connection.execute(EUROPEAN_PRICES_SQL)
    sample = ", ".join(row[0] for row in connection.execute(
        "select price from raw.listings where price like '%,__' order by id limit 3").fetchall())
    return (f"{changed} listing prices now arrive the European way (for example {sample}).\n"
            "Next: dbt build --target duckdb --profiles-dir . --full-refresh  Does any test fail?")


def drill_late_reviews(connection) -> str:
    already = connection.execute("select count(*) from raw.reviews where reviewer_name like 'Late Guest %' and comments = 'Posted late'").fetchone()[0]
    if already:
        return ("These 15 late reviews are already in RAW from an earlier drill.\n"
                "Next: dbt build --target duckdb --profiles-dir .  and compare raw and fact review counts.")
    before = count_in(connection, "reviews")
    listing = connection.execute("select listing_id from raw.reviews limit 1").fetchone()[0]
    connection.execute(late_reviews_sql(listing))
    after = count_in(connection, "reviews")
    return (f"15 reviews dated June 2015 just arrived: RAW.REVIEWS {before:,} -> {after:,} rows.\n"
            "Next: dbt build --target duckdb --profiles-dir .  (an incremental run, NOT --full-refresh), then compare counts.")


DRILLS = {"new-snapshot": drill_new_snapshot, "duplicate-days": drill_duplicate_days,
          "european-prices": drill_european_prices, "late-reviews": drill_late_reviews}


def run_drill(name: str) -> int:
    if name == "reset":
        loader = ROOT / "scripts" / "load_inside_airbnb_to_duckdb.py"
        done = subprocess.run([sys.executable, str(loader)], cwd=ROOT)
        if done.returncode == 0:
            print("RAW is back to the files in data/raw. Next: dbt build --target duckdb --profiles-dir . --full-refresh")
        return done.returncode
    connection = drill_connection()
    try:
        print(DRILLS[name](connection))
    finally:
        connection.close()
    print("(This changed YOUR local RAW tables on purpose. Put them back with: python scripts/lab_check.py drill reset)")
    return 0


CI_CHECKS = {"staging": ci_staging, "intermediate": ci_intermediate, "marts": ci_marts,
             "build-it": ci_build_it, "break-it": ci_break_it, "ship": ci_ship}
PROOF_MODES = {"env": mode_env, "raw": mode_raw}


def main() -> int:
    args = sys.argv[1:]
    if not (ROOT / "dbt_project.yml").exists():
        print("Run this from the project folder (the one with dbt_project.yml).")
        return 2
    if len(args) == 2 and args[0] == "ci" and args[1] in CI_CHECKS:
        report = Report(args[1])
        print(f"PrepNPlaced Lab check: {args[1]}", flush=True)
        try:
            CI_CHECKS[args[1]](report)
        except Exception as error:  # noqa: BLE001 — a model that failed to build must read as a red line, not a traceback
            first = (str(error).strip().splitlines() or [type(error).__name__])[0][:240]
            report.check(False, f"this check could not finish: {first}")
            print("    Usually a model of yours did not build. Open the 'Load RAW and build' step above, fix the first"
                  " red model it lists, push, and this check runs again.", flush=True)
        print(f"\n{'PASS' if report.ok else 'FAIL'}: {args[1]}", flush=True)
        return 0 if report.ok else 1
    if len(args) == 2 and args[0] == "drill" and (args[1] in DRILLS or args[1] == "reset"):
        return run_drill(args[1])
    if len(args) == 1 and args[0] in PROOF_MODES:
        block = PROOF_MODES[args[0]]()
        payload = {"project": PROJECT, "mode": args[0], **block}
        print("===== PREPNPLACED CHECKPOINT =====")
        print(json.dumps(payload, indent=2))
        print("===== END =====")
        print("\nCopy everything between the ===== lines into the checkpoint box on the site.")
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
