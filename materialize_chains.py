#!/usr/bin/env python3
"""Precompute citation chains and store them in the database.

Creates table `citation_paths(start_celex, end_celex, depth, path)` and
inserts every simple path up to `--max-depth` discovered in the `citations`
table. Run once and then serve the DB with Datasette for fast queries.
"""
import argparse
import sqlite3
from collections import defaultdict
from tqdm import tqdm


def load_edges(con):
    cur = con.cursor()
    # Build paragraph->paragraph edges from the citation_paragraphs table (more reliable)
    adj = defaultdict(set)
    terminals = defaultdict(set)  # node -> set of terminal celex
    nodes = set()

    # Paragraph-level edges: citation_paragraphs -> citations (gives cited paragraph)
    cur.execute("""
    SELECT c.source_celex, c.source_paragraph, cp.paragraph AS cited_paragraph, c.cited_celex
    FROM citation_paragraphs cp
    JOIN citations c ON cp.citation_id = c.id
    WHERE c.source_paragraph IS NOT NULL AND c.cited_celex IS NOT NULL
    """)
    for src_celex, src_para, cited_para, cited_celex in cur.fetchall():
        if not src_celex or not src_para or not cited_celex or not cited_para:
            continue
        src_key = f"{src_celex}#{src_para}"
        dst_key = f"{cited_celex}#{cited_para}"
        adj[src_key].add(dst_key)
        nodes.add(src_key)
        nodes.add(dst_key)

    # Terminal citations: citations that cite a case but no paragraph
    cur.execute("""
    SELECT source_celex, source_paragraph, cited_celex
    FROM citations
    WHERE source_paragraph IS NOT NULL AND (cited_paragraphs IS NULL OR trim(cited_paragraphs) = '') AND cited_celex IS NOT NULL
    """)
    for src_celex, src_para, cited_celex in cur.fetchall():
        if not src_celex or not src_para or not cited_celex:
            continue
        src_key = f"{src_celex}#{src_para}"
        terminals[src_key].add(cited_celex)
        nodes.add(src_key)

    return adj, terminals, nodes


def create_table(con):
    cur = con.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS citation_paths (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_celex TEXT NOT NULL,
        start_paragraph TEXT NOT NULL,
        end_celex TEXT,
        end_paragraph TEXT,
        depth INTEGER NOT NULL,
        path TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_cp_start ON citation_paths(start_celex, start_paragraph);
    CREATE INDEX IF NOT EXISTS idx_cp_end ON citation_paths(end_celex);
    """)
    con.commit()


def materialize(con, max_depth=6, batch_size=1000):
    adj, terminals, nodes = load_edges(con)
    create_table(con)
    cur = con.cursor()

    # Optional: clear existing rows
    cur.execute("DELETE FROM citation_paths")
    con.commit()

    insert_buf = []

    for start in tqdm(sorted(nodes), desc="Processing start nodes"):
        # Only start from paragraph nodes (contain '#')
        if '#' not in start:
            continue
        stack = [(start, [start])]
        while stack:
            node, path = stack.pop()
            # If this node has terminal citations, record those paths and do not continue from that terminal
            for term_celex in terminals.get(node, ()): 
                depth = len(path) - 1
                insert_buf.append((
                    path[0].split('#', 1)[0],
                    path[0].split('#', 1)[1],
                    term_celex,
                    None,
                    depth,
                    " -> ".join(path) + f" -> {term_celex}",
                ))
                if len(insert_buf) >= batch_size:
                    cur.executemany(
                        "INSERT INTO citation_paths (start_celex, start_paragraph, end_celex, end_paragraph, depth, path) VALUES (?, ?, ?, ?, ?, ?)",
                        insert_buf,
                    )
                    con.commit()
                    insert_buf.clear()

            if max_depth > 0 and len(path) - 1 >= max_depth:
                continue

            for nbr in adj.get(node, ()):  # paragraph neighbors
                if nbr in path:
                    continue
                new_path = path + [nbr]
                depth = len(new_path) - 1
                # nbr is like CELEX#PARA
                end_celex, end_para = nbr.split('#', 1)
                insert_buf.append((
                    path[0].split('#', 1)[0],
                    path[0].split('#', 1)[1],
                    end_celex,
                    end_para,
                    depth,
                    " -> ".join(new_path),
                ))
                if len(insert_buf) >= batch_size:
                    cur.executemany(
                        "INSERT INTO citation_paths (start_celex, start_paragraph, end_celex, end_paragraph, depth, path) VALUES (?, ?, ?, ?, ?, ?)",
                        insert_buf,
                    )
                    con.commit()
                    insert_buf.clear()
                stack.append((nbr, new_path))

    if insert_buf:
        cur.executemany(
            "INSERT INTO citation_paths (start_celex, start_paragraph, end_celex, end_paragraph, depth, path) VALUES (?, ?, ?, ?, ?, ?)",
            insert_buf,
        )
        con.commit()

    # Build canonical `chains` table and populate `chain_id` on citation_paths
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS chains (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain TEXT UNIQUE
    );
    """)
    con.commit()

    # Insert distinct paths into chains
    cur.execute("INSERT OR IGNORE INTO chains(chain) SELECT DISTINCT path FROM citation_paths WHERE path IS NOT NULL;")
    con.commit()

    # Add chain_id column to citation_paths if missing
    cols = [r[1] for r in cur.execute("PRAGMA table_info(citation_paths);").fetchall()]
    if 'chain_id' not in cols:
        cur.execute("ALTER TABLE citation_paths ADD COLUMN chain_id INTEGER;")
        con.commit()

    # Populate chain_id from chains table
    cur.execute("UPDATE citation_paths SET chain_id = (SELECT id FROM chains WHERE chains.chain = citation_paths.path) WHERE path IS NOT NULL;")
    con.commit()

    # Index for fast lookup
    cur.execute("CREATE INDEX IF NOT EXISTS idx_citation_paths_chain_id ON citation_paths(chain_id);")
    con.commit()


def main():
    p = argparse.ArgumentParser(description="Materialize citation chains into the DB")
    p.add_argument("--db", default="citations.db", help="SQLite DB path")
    p.add_argument("--max-depth", type=int, default=6, help="Maximum path length (edges); 0 = no limit")
    p.add_argument("--batch", type=int, default=1000, help="Insert batch size")
    args = p.parse_args()

    con = sqlite3.connect(args.db)
    try:
        materialize(con, max_depth=args.max_depth, batch_size=args.batch)
    finally:
        con.close()


if __name__ == "__main__":
    main()
