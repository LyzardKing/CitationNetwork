import json
import argparse
import logging
import requests
import zipfile
import io
import os
import xml.dom.minidom as minidom
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from datetime import datetime, timezone
from tqdm import tqdm
from SPARQLWrapper import SPARQLWrapper, JSON

from utils import setup_logger

# (GROUP_CONCAT(?subject_label; separator=", ") AS ?subject_labels)
#  || str(?legal_basis_celex) = "32016R0679" || str(?legal_basis_celex) = "31995L0046"

QUERY = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?case ?year ?celex ?ecli ?belonging_case_identifier ?domain ?title ?item (GROUP_CONCAT(distinct ?legal_basis_celex; separator=", ") AS ?legal_basis_cel) (GROUP_CONCAT(distinct ?cited_work_celex; separator=", ") AS ?cited_work_cel) ?m
WHERE {
    ?case cdm:resource_legal_id_celex ?celex .
    ?case cdm:case-law_ecli ?ecli .
    OPTIONAL {?belonging_case cdm:dossier_contains_work ?case.}
    OPTIONAL {?belonging_case cdm:case_court_domain ?domain.}
    OPTIONAL {?belonging_case cdm:case_court_identifier_court "C"^^<http://www.w3.org/2001/XMLSchema#string> .}
    optional {?belonging_case cdm:dossier_identifier ?belonging_case_identifier. }
    OPTIONAL {?belonging_case cdm:case_court_identifier_year ?year.}

    optional {?case cdm:resource_legal_is_about_subject-matter ?subject .}
    optional {?subject skos:prefLabel ?subject_label .
    FILTER (LANG(?subject_label) = "en").}

    optional {?case cdm:case-law_interpretes_resource_legal ?legal_basis .}
    optional {?legal_basis cdm:resource_legal_id_celex ?legal_basis_celex .}
    # # FILTER(str(?legal_basis_celex) = "32016R0679" || str(?legal_basis_celex) = "31995L0046").
    optional {?expression cdm:expression_belongs_to_work ?legal_basis .}
    optional {?expression cdm:expression_title ?legal_basis_title .
    ?expression cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> .}
    
    ?case cdm:work_cites_work ?cited_work .
    ?cited_work cdm:resource_legal_id_celex ?cited_work_celex .
    # FILTER(str(?cited_work_celex) = "32016R0679" || str(?cited_work_celex) = "31995L0046").

    ?e cdm:expression_belongs_to_work ?case.
    ?e cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG>.
    ?e cdm:expression_title ?title.
    FILTER(strstarts(str(?title), 'Judgment')).

    # OPTIONAL { 
        ?m cdm:manifestation_manifests_expression ?e.
        ?m cdm:manifestation_type "fmx4"^^<http://www.w3.org/2001/XMLSchema#string>.
        # ?m cdm:manifestation_has_item ?item.
    # }
}
# ORDER BY DESC(?date)
# LIMIT 10
"""

def query(save=False):
    print("Hello from privacytopic!")
    sparql = SPARQLWrapper("https://publications.europa.eu/webapi/rdf/sparql")
    sparql.setQuery(QUERY)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    BASE = "http://publications.europa.eu/resource"
    LANG = "ENG"

    output = []
    for result in results["results"]["bindings"]:
        item = {}
        for key, value in result.items():
            item[key] = value["value"]
        item["url"] = f"{BASE}/celex/{item['celex']}.{LANG}"
        item["formex"] = item["m"] + "/DOC_1"
        output.append(item)
            # print(f"{key}: {value['value']}")
        # print("\n")
        # name = result.get("name", {}).get("value")
        # date = result["date"]["value"]
        # case = result["case"]["value"]
        # print(f"Case: {case}\n  Name: {name}\n  Date: {date}\n")
    if save:
        json.dump(results["results"]["bindings"], open("results.json", "w"), indent=2)
        json.dump(output, open("output.json", "w"), indent=2)
    print("Found {} results.".format(len(results["results"]["bindings"])))


def exists_and_is_valid(filepath, valid_files, lock):
    if filepath in valid_files:
        return True
    if not os.path.exists(filepath):
        return False
    if os.path.getsize(filepath) == 0:
        return False
    try:
        with open(filepath, "r") as f:
            minidom.parse(f)
        with lock:
            valid_files.add(filepath)
        return True
    except Exception:
        return False


def _fetch_one(item, logger, valid_files, lock):
    """Download a single Formex XML file. Returns 'fetched', 'skipped', or 'failed'."""
    celex = item["celex"]
    filename = f"formex/{celex}.xml"
    if exists_and_is_valid(filename, valid_files, lock):
        item["formex_filepath"] = filename
        logger.debug("SKIP_EXISTING %s", celex)
        return "skipped"
    try:
        response = requests.get(
            item["formex"], headers={"Accept": "*/*"}, allow_redirects=True, timeout=60
        )
        if response.status_code != 200:
            logger.warning("FAILED %s HTTP %s", celex, response.status_code)
            return "failed"
        if response.content[:2] == b"PK":
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                xml_content = z.read(z.namelist()[0]).decode("utf-8")
        else:
            xml_content = response.text
        try:
            xml_content = minidom.parseString(xml_content).toprettyxml(indent="  ")
        except Exception as e:
            logger.error("FAILED %s XML parse error: %s", celex, e)
            return "failed"
        with open(filename, "w") as f:
            f.write(xml_content)        item["formex_filepath"] = filename
        logger.info("FETCHED %s", celex)
        return "fetched"
    except Exception as e:
        logger.error("FAILED %s %s", celex, e)
        if os.path.exists(filename):
            os.remove(filename)
        return "failed"


def fetch_formex(limit=None, workers=4):
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    logger = setup_logger("fetch_formex", log_path=f"formex/fetch_{run_ts}.log")

    with open("output.json", "r") as f:
        items = json.load(f)

    # Load the list of already validated files to skip them without needing to parse again.
    # This will contain a list of filepaths that have been successfully parsed as XML in previous runs, so we can skip them immediately without needing to parse them again.
    valid_files = set()
    if os.path.exists("valid_files.txt"):
        with open("valid_files.txt", "r") as f:
            valid_files = {line.rstrip("\n") for line in f if line.strip()}

    lock = threading.Lock()

    fetched = skipped = failed = 0

    logger.info("Starting fetch run for %d items (limit=%s, workers=%d)", len(items), limit, workers)

    executor = ThreadPoolExecutor(max_workers=workers)
    # Submit all items; _fetch_one handles the skip check itself so every
    # item advances the progress bar, matching the original sequential behaviour.
    futures = {executor.submit(_fetch_one, item, logger, valid_files, lock): item for item in items}

    try:
        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching Formex XML"):
            result = future.result()
            if result == "fetched":
                fetched += 1
            elif result == "skipped":
                skipped += 1
            else:
                failed += 1
            if limit is not None and fetched >= limit:
                for f in futures:
                    f.cancel()
                break
    except KeyboardInterrupt:
        logger.warning("Interrupted — cancelling pending downloads…")
        for f in futures:
            f.cancel()
        executor.shutdown(wait=False)
        raise
    else:
        executor.shutdown(wait=True)

    logger.info("Done. fetched=%d skipped_existing=%d failed=%d", fetched, skipped, failed)

    with open("valid_files.txt", "w") as vf:
        vf.write("\n".join(sorted(valid_files)))
        if valid_files:
            vf.write("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull cases from the EU Publications API.")
    parser.add_argument("--query", action="store_true", help="Run the SPARQL query to fetch cases.")
    parser.add_argument("--save", action="store_true", help="Save the results to a JSON file.")
    parser.add_argument("--fetch-formex", action="store_true", help="Fetch the Formex XML for the results.")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Max number of new files to download (default: all).")
    parser.add_argument("--workers", type=int, default=4, metavar="N",
                        help="Parallel download workers for --fetch-formex (default: 4).")
    args = parser.parse_args()
    if args.query:
        query(args.save)
    if args.fetch_formex:
        os.makedirs("formex", exist_ok=True)
        fetch_formex(limit=args.limit, workers=args.workers)
