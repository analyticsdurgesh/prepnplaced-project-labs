# PrepNPlaced Project Labs

Checkpoint checkers for the [PrepNPlaced](https://www.prepnplaced.com) Project Lab: the guided,
build-it-yourself curriculum over real data-engineering projects.

Each folder matches one project in the Lab.

| Project | Checker | How checkpoints are proved |
|---|---|---|
| Retail Revenue Intelligence | [`retail-revenue-intelligence/checkpoint.py`](retail-revenue-intelligence/checkpoint.py) | you paste the proof block it prints |
| Blinkit Warehouse Pipeline (Snowflake + dbt) | [`blinkit-dbt-pipeline/checkpoint.py`](blinkit-dbt-pipeline/checkpoint.py) | you paste the proof block it prints |
| Airbnb Analytics Engineering (dbt) | [`airbnb-snowflake-dbt/lab_check.py`](airbnb-snowflake-dbt/lab_check.py) | setup: proof block; build checkpoints: GitHub runs it in your fork |

## Pasted proof blocks

Download the project's checker into your clone's `scripts/` folder and run it from the project folder:

```bash
python scripts/checkpoint.py <mode>      # Retail, Blinkit
python scripts/lab_check.py env          # Airbnb setup checkpoints: env, raw
```

It prints a block between `=====` lines to paste into the checkpoint box on the site.

## Checkpoints proved by GitHub (Airbnb)

Your fork of the project carries a workflow, `.github/workflows/prepnplaced-lab.yml`, called
**PrepNPlaced Lab check**. On every push it builds your project on a clean machine, downloads
`lab_check.py` from this repository **at a pinned commit** (and checks its SHA-256), and runs one step
per checkpoint. You paste your fork's URL on the site; the site reads the step's result from GitHub,
and only counts runs of the unedited workflow file.

Run any of those checks yourself before you push, after a full `dbt build --target duckdb --profiles-dir .`:

```bash
python scripts/lab_check.py ci staging      # or intermediate, marts, build-it, break-it, ship
```

Module 4 and Module 6 also use `python scripts/lab_check.py drill <name>` to break your own data on
purpose (and `drill reset` to put it back).

## What the checkers read

Only your project: its files, dbt's `target/` artifacts and the local DuckDB file. To test your tests,
the Airbnb checker copies the database into a temporary folder and breaks the copy, never your own
files. No checker opens `profiles.yml`, reads credentials, or sends anything anywhere.

Found a problem? PRs welcome.
