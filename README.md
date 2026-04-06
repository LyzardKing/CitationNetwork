CJEU Privacy Citation Network — Setup and DB build

This document explains how to build the SQLite database from the provided Formex XML files and serve it with Datasette.

Prerequisites
- Python 3.11+ (see `pyproject.toml` for exact requirements)
- Git (optional)

Quick setup

1. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install the project dependencies

```bash
pip install -e .
```

Build the database (from zero)

1. Parse Formex XML files and save to `citations.db` (this creates `citations`, `citation_paragraphs`, `cases`, `paragraphs`, and `chains` schema entries)

```bash
# This will reset the DB and populate it from the `formex/` folder
python3 parse_citations.py --reset
```

2. Materialize paragraph-level citation chains and create canonical `chains` table

```bash
# --max-depth 0 means unlimited depth (may take a long time)
python3 materialize_chains.py --db citations.db --max-depth 0
```

Notes:
- `materialize_chains.py` will create/append to the `citation_paths` table and then create/populate the `chains` table and write `chain_id` into `citation_paths`.
- Use `--max-depth` to limit traversal if you want faster, shallower results.

Serve with Datasette

```bash
# Start Datasette and use the provided metadata.yml for saved queries and UI customization
datasette citations.db --metadata metadata.yml
# OR, if you prefer the uv tool used in this workspace
uv run datasette citations.db --metadata metadata.yml
```

Usage tips

- List canonical chains (first 20):

```bash
sqlite3 citations.db "SELECT id, substr(chain,1,200) AS preview FROM chains ORDER BY id LIMIT 20;"
```

- See chain occurrences that start from a specific paragraph:

```bash
# via sqlite3
sqlite3 citations.db "SELECT * FROM citation_paths WHERE start_celex='61961CJ0009' AND start_paragraph='(b)' ORDER BY depth DESC LIMIT 100;"
```

- Use the Datasette UI saved queries:
	- `Paragraphs (view chains)` to list paragraphs with links to chains
	- `Citation chains (precomputed)` to list chains (or pass `start_celex`/`start_paragraph` params)
	- `Chain detail` expects a `chain_id` and shows one row per element of the chain

Troubleshooting

- If `chain_detail` returns errors, ensure `chains` has entries:

```bash
sqlite3 citations.db "SELECT COUNT(*) FROM chains;"
```

- Rebuild from scratch:

```bash
python3 parse_citations.py --reset
python3 materialize_chains.py --db citations.db --max-depth 0
```

Contact / Next steps
- If you want paragraphs normalized or alternative chain tokenization, I can add normalization passes in `materialize_chains.py` before building chains.

oxigraph convert --from-file citations.ttl --to-file citations.nt