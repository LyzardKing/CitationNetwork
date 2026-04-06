import hashlib
import json
import logging
import os
import re
import sqlite3
import xml.etree.ElementTree as ET
import pandas as pd
import argparse
from tqdm import tqdm

from utils import setup_logger


logger = setup_logger("parse_citations")

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
    logger.debug("Found %d citations in %s", len(citations), filepath)
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

        # Resolve cited identifiers.
        # - ECLI is treated as a single token (do not split).
        # - NO.CASE may contain multiple case numbers (e.g. separated by '+');
        #   try each token until a matching case is found.
        cited_ids = {"celex": "", "ecli": "", "no_case": ""}
        first_cited_no = ""
        if cited_ecli:
            # Use ECLI as provided; do not attempt to split it.
            cited_ids = get_identifiers(cited_ecli, known={"ecli": cited_ecli})
        elif cited_no_case:
            # Split NO.CASE on '+' and try tokens in order.
            tokens = cited_no_case.split("+")
            first_cited_no = tokens[0] if tokens else ""
            for tok in tokens:
                tok = tok.split(" ")[0]
                ids = get_identifiers(tok, known={"no_case": tok})
                if ids.get("celex") or ids.get("ecli") or ids.get("no_case"):
                    cited_ids = ids
                    break
            # Fallback: if nothing matched, try the first token as anchor
            if not (cited_ids["celex"] or cited_ids["ecli"] or cited_ids["no_case"]) and first_cited_no:
                cited_ids = get_identifiers(first_cited_no, known={"no_case": first_cited_no})

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

    logger.debug("Parsed %d citations from %s", len(result), filepath)
    return result


def parse_paragraphs(filepath):
    """Extract all paragraphs (celex, paragraph_no, paragraph_text) from a Formex XML file."""
    celex = os.path.splitext(os.path.basename(filepath))[0]
    tree = ET.parse(filepath)
    root = tree.getroot()
    result = []
    for node in root.iter():
        if node.tag == "NP.ECR":
            identifier = node.get("IDENTIFIER", "").strip()
            no_p = node.findtext("NO.P", "").strip()
            txt_el = node.find("TXT")
            raw = "".join((txt_el if txt_el is not None else node).itertext())
            text = " ".join(raw.split())
            if identifier:
                result.append({"celex": celex, "paragraph": identifier, "paragraph_text": text})
            if no_p and no_p != identifier:
                result.append({"celex": celex, "paragraph": no_p, "paragraph_text": text})
        elif node.tag == "NP":
            no_p = node.findtext("NO.P", "").strip()
            if no_p:
                txt_el = node.find("TXT")
                raw = "".join((txt_el if txt_el is not None else node).itertext())
                text = " ".join(raw.split())
                result.append({"celex": celex, "paragraph": no_p, "paragraph_text": text})
    return result


def _folder_cache_key(folder):
    """Hash of sorted (filename, mtime, size) for every XML in *folder*."""
    h = hashlib.md5()
    for name in sorted(f for f in os.listdir(folder) if f.endswith(".xml")):
        st = os.stat(os.path.join(folder, name))
        h.update(f"{name}:{st.st_mtime_ns}:{st.st_size}\n".encode())
    return h.hexdigest()


def parse_files(folder, cache_path=None):
    if cache_path is None:
        cache_path = os.path.join(folder, ".parse_cache.json")

    key = _folder_cache_key(folder)
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r") as f:
                cached = json.load(f)
            if cached.get("key") == key:
                logger.info("parse_files: loaded %d rows from cache", len(cached["rows"]))
                return cached["rows"]
        except Exception as e:
            logger.warning("parse_files: ignoring unreadable cache (%s)", e)

    all_rows = []
    filenames = sorted(f for f in os.listdir(folder) if f.endswith(".xml"))
    for filename in tqdm(filenames, desc="Parsing XML files"):
        filepath = os.path.join(folder, filename)
        rows = parse_citations(filepath)
        all_rows.extend(rows)
        logger.debug("Parsed from %s", filename)

    try:
        with open(cache_path, "w") as f:
            json.dump({"key": key, "rows": all_rows}, f)
        logger.info("parse_files: cached %d rows to %s", len(all_rows), cache_path)
    except Exception as e:
        logger.warning("parse_files: could not write cache (%s)", e)

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
    logger.info("Reset %s", db_path)


def save_to_db(folder, db_path="citations.db"):
    con = sqlite3.connect(db_path)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS citations (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            source_celex          TEXT NOT NULL,
            source_paragraph      TEXT,
            cited_celex           TEXT,
            cited_paragraphs      TEXT
        );
        CREATE TABLE IF NOT EXISTS citation_paragraphs (
            citation_id  INTEGER NOT NULL REFERENCES citations(id),
            paragraph    TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS chains (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain TEXT UNIQUE
        );
        CREATE TABLE IF NOT EXISTS cases (
            celex    TEXT PRIMARY KEY,
            ecli     TEXT,
            no_case  TEXT,
            title    TEXT,
            year     TEXT,
            domain   TEXT,
            legal_basis TEXT,
            cited_works TEXT
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

    # Normalize paragraph identifiers by stripping leading zeros (e.g. "NP0047" → "47")
    for row in rows:
        if row["source_paragraph"]:
            row["source_paragraph"] = row["source_paragraph"].removeprefix("NP").lstrip("0")
        # row["cited_paragraphs"] = [p.removeprefix("NP").lstrip("0") for p in row["cited_paragraphs"]]

    # Insert citations using the simplified schema
    for row in rows:
        cur.execute(
            "INSERT INTO citations (source_celex, source_paragraph, cited_celex, cited_paragraphs) VALUES (?, ?, ?, ?)",
            (row["source_celex"],
             row["source_paragraph"] or None,
             row["cited_celex"] or None,
             ",".join(row["cited_paragraphs"]) or None),
        )
        cit_id = cur.lastrowid
        for para in row["cited_paragraphs"]:
            cur.execute(
                "INSERT INTO citation_paragraphs (citation_id, paragraph) VALUES (?, ?)",
                (cit_id, para),
            )

    # populate paragraphs table from all source XML files
    para_filenames = sorted(f for f in os.listdir(folder) if f.endswith(".xml"))
    for filename in tqdm(para_filenames, desc="Indexing paragraphs"):
        para_rows = parse_paragraphs(os.path.join(folder, filename))
        for pr in para_rows:
            cur.execute(
                "INSERT OR IGNORE INTO paragraphs (celex, paragraph, paragraph_text) VALUES (?, ?, ?)",
                (pr["celex"], pr["paragraph"], pr["paragraph_text"]),
            )

    # Populate cases table from output.json (contains domain, legal_basis, cited works etc.)
    output_json = os.path.join(os.path.dirname(__file__), "output.json")
    try:
        with open(output_json, "r") as f:
            out = json.load(f)
    except Exception:
        out = []

    for entry in out:
        celex = entry.get("celex") or entry.get("CELEX")
        if not celex:
            continue
        ecli = entry.get("ecli") or entry.get("ECLI")
        no_case = entry.get("belonging_case_identifier") or entry.get("no_case") or entry.get("CASE_NO")
        title = entry.get("title")
        year = entry.get("year")
        domain = entry.get("domain")
        legal_basis = entry.get("legal_basis_cel") or entry.get("legal_basis")
        cited_works = entry.get("cited_work_cel") or entry.get("cited_works")

        cur.execute(
            "INSERT OR IGNORE INTO cases (celex, ecli, no_case, title, year, domain, legal_basis, cited_works) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (celex or None, ecli or None, no_case or None, title or None, year or None, domain or None, legal_basis or None, cited_works or None)
        )

    # Additionally, merge in cases from cases.csv for any missing entries
    db = _load_db()
    for _, row in db.iterrows():
        cur.execute(
            "INSERT OR IGNORE INTO cases (celex, ecli, no_case, title) VALUES (?, ?, ?, ?)",
            (row["CELEX"] or None, row["ECLI"] or None, row["CASE_NO"] or None, row["TITLE"] or None)
        )

    con.commit()
    con.close()
    logger.info("Saved %d citations to %s", len(rows), db_path)


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
        if rows.empty:
            # Try matching common variants: contain the token (e.g. "C-463/10 P")
            try:
                rows = db[db["CASE_NO"].str.contains(r"\b" + re.escape(clean) + r"\b", na=False)]
            except Exception:
                rows = db[db["CASE_NO"].str.contains(re.escape(clean), na=False)]
    else:
        # CELEX
        rows = db[db["CELEX"] == identifier]

    if not rows.empty:
        row = rows.iloc[0]
        result["celex"]   = result["celex"]   or row["CELEX"]
        result["ecli"]    = result["ecli"]    or row["ECLI"]
        result["no_case"] = result["no_case"] or row["CASE_NO"]

    return result


_ELI  = "http://data.europa.eu/eli/ontology#"
_CIT  = "https://example.org/cjeu/citation#"
_BASE = "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri="
# Paragraph identifiers must contain at least one digit and consist only of
# characters that are safe as a Turtle URI fragment (no spaces, parens, etc.).
# Pure-letter labels ("B", "II") and sub-letter qualifiers ("1. (b)", "2 (d)")
# are section markers or complex references that cannot be cleanly represented
# as URI fragments and are skipped.
def _valid_para_id(s):
    return bool(s) and bool(re.search(r'\d', s)) and bool(re.fullmatch(r'[A-Za-z0-9._\-]+', s))


def _judgment_uri(ecli=None, celex=None):
    from rdflib import URIRef
    if ecli:
        return URIRef(f"{_BASE}ecli:{ecli}")
    if celex:
        return URIRef(f"{_BASE}CELEX:{celex}")
    return None


def save_to_rdf(folder, output="citations.ttl"):
    from rdflib import Graph, URIRef, Literal, Namespace, RDF
    from rdflib.namespace import DCTERMS

    CIT = Namespace(_CIT)
    ELI = Namespace(_ELI)

    g = Graph()
    g.bind("cit", CIT)
    g.bind("eli", ELI)
    g.bind("dcterms", DCTERMS)

    # paragraph_texts will not be written to JSON; paragraph text is emitted
    # directly into the RDF graph when available.

    # Parse citations first so we know which judgments actually appear
    rows = parse_files(folder)

    # Build ECLI -> CELEX lookup for cases where only ECLI is present on a citation.
    db = _load_db()
    ecli_to_celex = {
        row["ECLI"]: row["CELEX"]
        for _, row in db.iterrows()
        if row["ECLI"] and row["CELEX"]
    }

    # Mirror save_to_db paragraph extraction so paragraph nodes can carry text.
    paragraph_text_by_key = {}
    para_filenames = sorted(f for f in os.listdir(folder) if f.endswith(".xml"))
    for filename in tqdm(para_filenames, desc="Indexing paragraphs"):
        para_rows = parse_paragraphs(os.path.join(folder, filename))
        for pr in para_rows:
            paragraph_text_by_key[(pr["celex"], pr["paragraph"])] = pr["paragraph_text"]

    # Collect only the judgment URIs that are referenced in the citation network
    used_uris = set()
    for row in rows:
        src_j = _judgment_uri(ecli=row["source_ecli"] or None, celex=row["source_celex"])
        cit_j = _judgment_uri(ecli=row["cited_ecli"] or None, celex=row["cited_celex"] or None)
        if src_j:
            used_uris.add(str(src_j))
        if cit_j:
            used_uris.add(str(cit_j))

    # Emit metadata only for judgments that appear in the citation network
    # Emit metadata for judgments. Include entries from the cases DB and
    # from output.json so RDF encodes the same information as the SQLite DB.
    for _, row in tqdm(db.iterrows(), desc="Emitting judgment metadata from cases.csv"):
        j_uri = _judgment_uri(ecli=row["ECLI"] or None, celex=row["CELEX"] or None)
        if j_uri is None:
            continue
        g.add((j_uri, RDF.type, ELI.LegalResource))
        if row["CELEX"]:
            g.add((j_uri, ELI.id_local, Literal(row["CELEX"])))
        if row["ECLI"]:
            g.add((j_uri, DCTERMS.identifier, Literal(row["ECLI"])))
        if row["CASE_NO"]:
            g.add((j_uri, ELI.number, Literal(row["CASE_NO"])))
        if row["TITLE"]:
            g.add((j_uri, DCTERMS.title, Literal(row["TITLE"], lang="en")))

    # Also include metadata from output.json (contains extra fields like year, domain, legal_basis, cited_works)
    output_json = os.path.join(os.path.dirname(__file__), "output.json")
    try:
        with open(output_json, "r") as f:
            out = json.load(f)
    except Exception:
        out = []

    for entry in out:
        celex = entry.get("celex") or entry.get("CELEX")
        if not celex:
            continue
        j_uri = _judgment_uri(celex=celex)
        if j_uri is None:
            continue
        g.add((j_uri, RDF.type, ELI.LegalResource))
        ecli = entry.get("ecli") or entry.get("ECLI")
        if ecli:
            g.add((j_uri, DCTERMS.identifier, Literal(ecli)))
        no_case = entry.get("belonging_case_identifier") or entry.get("no_case") or entry.get("CASE_NO")
        if no_case:
            g.add((j_uri, ELI.number, Literal(no_case)))
        title = entry.get("title")
        if title:
            g.add((j_uri, DCTERMS.title, Literal(title, lang="en")))
        year = entry.get("year")
        if year:
            g.add((j_uri, CIT.year, Literal(year)))
        domain = entry.get("domain")
        if domain:
            g.add((j_uri, CIT.domain, Literal(domain)))
        legal_basis = entry.get("legal_basis_cel") or entry.get("legal_basis")
        if legal_basis:
            g.add((j_uri, CIT.legalBasis, Literal(legal_basis)))
        cited_works = entry.get("cited_work_cel") or entry.get("cited_works")
        if cited_works:
            g.add((j_uri, CIT.citedWorks, Literal(cited_works)))

    # Add citation triples
    for row in tqdm(rows, desc="Building citation triples"):
        src_j = _judgment_uri(ecli=row["source_ecli"] or None, celex=row["source_celex"])
        cit_j = _judgment_uri(ecli=row["cited_ecli"] or None, celex=row["cited_celex"] or None)

        if src_j is None:
            continue

        # Source paragraph node
        src_para_no = row["source_paragraph"]
        if src_para_no and _valid_para_id(src_para_no):
            src_p = URIRef(f"{src_j}#{src_para_no}")
            g.add((src_p, RDF.type, CIT.Paragraph))
            g.add((src_p, CIT.ofJudgment, src_j))
            g.add((src_j, CIT.contains, src_p))
            g.add((src_p, CIT.paragraphNumber, Literal(src_para_no)))
            # src_para_text = paragraph_text_by_key.get((row["source_celex"], src_para_no))
            # if src_para_text:
            #     g.add((src_p, CIT.paragraphText, Literal(src_para_text)))
        else:
            src_p = src_j

        if cit_j is not None:
            if row["cited_paragraphs"]:
                for para_no in row["cited_paragraphs"]:
                    clean_para_no = para_no.strip()
                    if not _valid_para_id(clean_para_no):
                        continue
                    cited_p = URIRef(f"{cit_j}#{clean_para_no}")
                    g.add((cited_p, RDF.type, CIT.Paragraph))
                    g.add((cited_p, CIT.ofJudgment, cit_j))
                    g.add((cit_j, CIT.contains, cited_p))
                    g.add((cited_p, CIT.paragraphNumber, Literal(clean_para_no)))
                    cited_celex_for_text = row["cited_celex"] or ecli_to_celex.get(row["cited_ecli"], "")
                    cited_para_text = paragraph_text_by_key.get((cited_celex_for_text, clean_para_no))
                    if cited_para_text:
                        g.add((cited_p, CIT.paragraphText, Literal(cited_para_text)))
                    g.add((src_p, CIT.cites, cited_p))
            else:
                g.add((src_p, CIT.cites, cit_j))

    g.serialize(output, format="turtle")
    logger.info("Saved %d citations to %s (%d triples)", len(rows), output, len(g))

    # text_output = os.path.splitext(output)[0] + "_text.json"
    # with open(text_output, "w", encoding="utf-8") as f:
    #     json.dump(paragraph_texts, f, ensure_ascii=False)
    # logger.info("Saved %d paragraph texts to %s", len(paragraph_texts), text_output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse citations from Formex XML files")
    parser.add_argument("--reset", action="store_true", help="Reset the database before saving (db mode only)")
    parser.add_argument("--output", choices=["db", "rdf"], default="db",
                        help="Output format: 'db' for SQLite (default), 'rdf' for Turtle RDF")
    parser.add_argument("--rdf-file", default="citations.ttl",
                        help="Output filename for RDF (default: citations.ttl)")
    args = parser.parse_args()
    if args.output == "rdf":
        save_to_rdf(folder="formex", output=args.rdf_file)
    else:
        if args.reset:
            reset_db()
        save_to_db(folder="formex")