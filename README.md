# PrepNPlaced Project Labs

Checkpoint checker scripts for the [PrepNPlaced](https://www.prepnplaced.com)
Project Lab — the guided, build-it-yourself curriculum over real data-engineering
projects.

Each folder matches one project in the Lab. Students download the project's
`checkpoint.py` into their clone's `scripts/` folder and run:

```bash
python scripts/checkpoint.py <mode>
```

It prints a proof block to paste into the checkpoint box on the site. The
scripts only READ your local project (and localhost for the dashboard check) —
they never upload anything.

| Project | Script |
|---|---|
| Retail Revenue Intelligence | [`retail-revenue-intelligence/checkpoint.py`](retail-revenue-intelligence/checkpoint.py) |

More projects arrive as their curricula go live. Found a problem? PRs welcome.
