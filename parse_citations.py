import os
import re
import sqlite3
import xml.etree.ElementTree as ET
import pandas as pd
import argparse

_CASES_CSV = os.path.join(os.path.dirname(__file__), "cases.csv")
_db = None

def _load_db():
    global _db
    if _db is None:
        df = pd.read_csv(_CASES_CSV, sep="|", na_filter=False,
                         usecols=["CELEX", "ECLI", "CASE_NO", "TITLE"])
        _db = df
    return _db


def parse_citations(filepath):
    source_celex = os.path.splitext(os.path.basename(filepath))[0]

    tree = ET.parse(filepath)
    root = tree.getroot()

    # Build child -> parent map (ElementTree has no getparent())
    parent_map = {child: parent for parent in root.iter() for child in parent}

    citations = root.findall(".//REF.DOC.ECR")
    print(f"Found {len(citations)} citations in {filepath}")
    result = []
    for citation in citations:
        # Walk up to find the nearest NP.ECR or NP ancestor
        node = citation
        source_paragraph = ""
        para_node = None
        while node is not None:
            node = parent_map.get(node)
            if node is None:
                break
            if node.tag == "NP.ECR":
                # Newer format: IDENTIFIER attribute e.g. "NP0047"
                source_paragraph = node.get("IDENTIFIER", "")
                para_node = node
                break
            if node.tag == "NP":
                # Older format: paragraph number in <NO.P> child text
                no_p = node.findtext("NO.P", "").strip()
                if no_p:
                    source_paragraph = no_p
                para_node = node
                break

        # NO.CASE and ECLI can appear as child elements or as attributes on the element itself
        cited_no_case = (citation.findtext("NO.CASE")
                         or citation.get("NO.CASE", ""))
        ecli_el = citation.find("NO.ECLI")
        cited_ecli = (ecli_el.get("ECLI", "") if ecli_el is not None
                      else citation.get("ECLI", ""))

        # Prefer explicit REF.NP.ECR child elements when present;
        # fall back to parsing tail text for patterns like "paragraph 39",
        # "paragraphs 54 and 55", or "paragraphs 39 to 42".
        ref_np_els = citation.findall("REF.NP.ECR")
        if ref_np_els:
            cited_paragraphs = [el.text.strip() for el in ref_np_els if el.text and el.text.strip().isdigit()]
        else:
            tail = citation.tail or ""
            m = re.search(r'\bparagraphs?\s+(\d+(?:\s*(?:,|and|to)\s*\d+)*)', tail)
            if m:
                tokens = re.split(r'\s*(?:,|and)\s*', m.group(1))
                cited_paragraphs = []
                for token in tokens:
                    rng = re.fullmatch(r'(\d+)\s+to\s+(\d+)', token.strip())
                    if rng:
                        cited_paragraphs.extend(str(n) for n in range(int(rng.group(1)), int(rng.group(2)) + 1))
                    elif token.strip().isdigit():
                        cited_paragraphs.append(token.strip())
            else:
                cited_paragraphs = []

        cited_no_case = cited_no_case.strip()
        cited_ecli = cited_ecli.strip()

        # Resolve cited identifiers, keeping whichever fields are already present
        first_cited_no = re.split(r'\s*(?:,|and)\s*', cited_no_case)[0].strip() if cited_no_case else ""
        cited_anchor = cited_ecli or first_cited_no
        cited_ids = get_identifiers(cited_anchor, known={"ecli": cited_ecli, "no_case": first_cited_no})
        source_ids = get_identifiers(source_celex)

        result.append({
            "source_celex": source_celex,
            "source_ecli": source_ids["ecli"],
            "source_no_case": source_ids["no_case"],
            "source_paragraph": source_paragraph,
            "cited_celex": cited_ids["celex"],
            "cited_ecli": cited_ids["ecli"] or cited_ecli,
            "cited_no_case": cited_no_case,
            "cited_paragraphs": cited_paragraphs,
        })

    print(f"Parsed {len(result)} citations from {filepath}")
    print(result)
    return result


def parse_paragraphs(filepath):
    """Extract all paragraphs (celex, paragraph_no, paragraph_text) from a Formex XML file."""
    celex = os.path.splitext(os.path.basename(filepath))[0]
    tree = ET.parse(filepath)
    root = tree.getroot()
    result = []
    for node in root.iter():
        if node.tag == "NP.ECR":
            no_p = node.findtext("NO.P", "").strip()
            txt_el = node.find("TXT")
            raw = "".join((txt_el if txt_el is not None else node).itertext())
            text = " ".join(raw.split())
            if no_p:
                result.append({"celex": celex, "paragraph": no_p, "paragraph_text": text})
        elif node.tag == "NP":
            no_p = node.findtext("NO.P", "").strip()
            if no_p:
                txt_el = node.find("TXT")
                raw = "".join((txt_el if txt_el is not None else node).itertext())
                text = " ".join(raw.split())
                result.append({"celex": celex, "paragraph": no_p, "paragraph_text": text})
    return result


def parse_files(folder):
    all_rows = []
    for filename in sorted(os.listdir(folder)):
        if not filename.endswith(".xml"):
            continue
        filepath = os.path.join(folder, filename)
        rows = parse_citations(filepath)
        all_rows.extend(rows)
        print(f"Parsed from {filename}")
    return all_rows


def reset_db(db_path="citations.db"):
    con = sqlite3.connect(db_path)
    con.executescript("""
        DROP TABLE IF EXISTS citation_paragraphs;
        DROP TABLE IF EXISTS citations;
        DROP TABLE IF EXISTS paragraphs;
        DROP TABLE IF EXISTS cases;
    """)
    con.commit()
    con.close()
    print(f"Reset {db_path}")


def save_to_db(folder, db_path="citations.db"):
    con = sqlite3.connect(db_path)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS citations (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            source_celex          TEXT NOT NULL,
            source_ecli           TEXT,
            source_no_case        TEXT,
            source_paragraph      TEXT,
            cited_celex           TEXT,
            cited_ecli            TEXT,
            cited_no_case         TEXT,
            cited_paragraphs      TEXT
        );
        CREATE TABLE IF NOT EXISTS citation_paragraphs (
            citation_id  INTEGER NOT NULL REFERENCES citations(id),
            paragraph    TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS cases (
            celex    TEXT PRIMARY KEY,
            ecli     TEXT,
            no_case  TEXT,
            title    TEXT
        );
        CREATE TABLE IF NOT EXISTS paragraphs (
            celex          TEXT NOT NULL,
            paragraph      TEXT NOT NULL,
            paragraph_text TEXT,
            PRIMARY KEY (celex, paragraph)
        );
        CREATE INDEX IF NOT EXISTS idx_cit_source ON citations(source_celex);
        CREATE INDEX IF NOT EXISTS idx_cit_cited  ON citations(cited_celex);
        CREATE INDEX IF NOT EXISTS idx_cp_citation ON citation_paragraphs(citation_id);
    """)

    cur = con.cursor()
    rows = parse_files(folder)
    for row in rows:
        cur.execute(
            "INSERT INTO citations "
            "(source_celex, source_ecli, source_no_case, source_paragraph, cited_celex, cited_ecli, cited_no_case, cited_paragraphs) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (row["source_celex"],
             row["source_ecli"] or None,
             row["source_no_case"] or None,
             row["source_paragraph"] or None,
             row["cited_celex"] or None,
             row["cited_ecli"] or None,
             row["cited_no_case"] or None,
             ",".join(row["cited_paragraphs"]) or None),
        )
        cit_id = cur.lastrowid
        for para in row["cited_paragraphs"]:
            cur.execute(
                "INSERT INTO citation_paragraphs (citation_id, paragraph) VALUES (?, ?)",
                (cit_id, para),
            )

    # populate paragraphs table from all source XML files
    for filename in sorted(os.listdir(folder)):
        if not filename.endswith(".xml"):
            continue
        para_rows = parse_paragraphs(os.path.join(folder, filename))
        for pr in para_rows:
            cur.execute(
                "INSERT OR IGNORE INTO paragraphs (celex, paragraph, paragraph_text) VALUES (?, ?, ?)",
                (pr["celex"], pr["paragraph"], pr["paragraph_text"]),
            )

    # populate cases table from the cases.csv file for easier lookup
    db = _load_db()
    for _, row in db.iterrows():
        cur.execute(
            "INSERT OR IGNORE INTO cases (celex, ecli, no_case, title) VALUES (?, ?, ?, ?)",
            (row["CELEX"] or None, row["ECLI"] or None, row["CASE_NO"] or None, row["TITLE"] or None)
        )

    con.commit()
    con.close()
    print(f"Saved {len(rows)} citations to {db_path}")


def get_identifiers(identifier, known=None):
    """Resolve identifier to {celex, ecli, no_case} using cases.csv only.

    `known` may contain already-resolved values; those are kept as-is.
    Only empty fields are looked up.
    """
    result = {"celex": "", "ecli": "", "no_case": ""}
    if known:
        result.update({k: v for k, v in known.items() if v})

    if all(result.values()) or not identifier:
        return result

    db = _load_db()

    if identifier.startswith("ECLI:"):
        rows = db[db["ECLI"] == identifier]
    elif "/" in identifier:
        # Case number — strip any court prefix (e.g. "C-107/98" → "107/98")
        clean = re.sub(r'^[A-Za-z]+-', '', identifier)
        rows = db[db["CASE_NO"] == clean]
    else:
        # CELEX
        rows = db[db["CELEX"] == identifier]

    if not rows.empty:
        row = rows.iloc[0]
        result["celex"]   = result["celex"]   or row["CELEX"]
        result["ecli"]    = result["ecli"]    or row["ECLI"]
        result["no_case"] = result["no_case"] or row["CASE_NO"]

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse citations from Formex XML files and save to SQLite database")
    parser.add_argument("--reset", action="store_true", help="Reset the database before saving")
    args = parser.parse_args()
    if args.reset:
        reset_db()
    save_to_db(folder="formex")