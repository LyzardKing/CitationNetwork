import json
import argparse
import logging
import requests
import zipfile
import io
import os
import xml.dom.minidom as minidom

from datetime import datetime, timezone
from tqdm import tqdm
from SPARQLWrapper import SPARQLWrapper, JSON

# (GROUP_CONCAT(?subject_label; separator=", ") AS ?subject_labels)
#  || str(?legal_basis_celex) = "32016R0679" || str(?legal_basis_celex) = "31995L0046"

QUERY = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?case ?celex ?ecli ?belonging_case_identifier ?domain ?title ?item (GROUP_CONCAT(distinct ?legal_basis_celex; separator=", ") AS ?legal_basis_cel) (GROUP_CONCAT(distinct ?cited_work_celex; separator=", ") AS ?cited_work_cel) ?m
WHERE {
    ?case cdm:resource_legal_id_celex ?celex .
    ?case cdm:case-law_ecli ?ecli .
    OPTIONAL {?belonging_case cdm:dossier_contains_work ?case.}
    OPTIONAL {?belonging_case cdm:case_court_domain ?domain.}
    OPTIONAL {?belonging_case cdm:case_court_identifier_court "C"^^<http://www.w3.org/2001/XMLSchema#string> .}
    optional {?belonging_case cdm:dossier_identifier ?belonging_case_identifier. }

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
    FILTER(str(?cited_work_celex) = "32016R0679" || str(?cited_work_celex) = "31995L0046").

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


def setup_logger(log_path):
    class TqdmHandler(logging.StreamHandler):
        def emit(self, record):
            tqdm.write(self.format(record))

    logger = logging.getLogger("fetch_formex")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
    fmt.converter = lambda *_: datetime.now(timezone.utc).timetuple()
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(fmt)
    stream_handler = TqdmHandler()
    stream_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def fetch_formex():
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    logger = setup_logger(f"formex/fetch_{run_ts}.log")

    with open("output.json", "r") as f:
        items = json.load(f)

    fetched = skipped = failed = 0

    logger.info("Starting fetch run for %d items", len(items))
    for item in tqdm(items, desc="Fetching Formex XML"):
        celex = item["celex"]
        filename = f"formex/{celex}.xml"
        if os.path.exists(filename):
            item["formex_filepath"] = filename
            logger.debug("SKIP_EXISTING %s", celex)
            skipped += 1
            continue
        response = requests.get(item["formex"], headers={"Accept": "*/*"}, allow_redirects=True)
        if response.status_code != 200:
            logger.warning("FAILED %s HTTP %s", celex, response.status_code)
            failed += 1
            continue
        if response.content[:2] == b"PK":
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                xml_content = z.read(z.namelist()[0]).decode("utf-8")
        else:
            xml_content = response.text
        with open(filename, "w") as f:
            try:
                xml_content = minidom.parseString(xml_content).toprettyxml(indent="  ")
            except Exception as e:
                logger.error("FAILED %s XML parse error: %s", celex, e)
                failed += 1
                if os.path.exists(filename):
                    os.remove(filename)
                continue
            f.write(xml_content)
        item["formex_filepath"] = filename
        logger.info("FETCHED %s", celex)
        fetched += 1

    logger.info("Done. fetched=%d skipped_existing=%d failed=%d", fetched, skipped, failed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull cases from the EU Publications API.")
    parser.add_argument("--query", action="store_true", help="Run the SPARQL query to fetch cases.")
    parser.add_argument("--save", action="store_true", help="Save the results to a JSON file.")
    parser.add_argument("--fetch-formex", action="store_true", help="Fetch the Formex XML for the results.")
    args = parser.parse_args()
    if args.query:
        query(args.save)
    if args.fetch_formex:
        os.makedirs("formex", exist_ok=True)
        fetch_formex()
