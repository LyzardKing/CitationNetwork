from SPARQLWrapper import SPARQLWrapper, JSON

from casetoecli import search_case_no

CACHE_CSV = "cases.csv"

QUERY = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
    SELECT distinct ?celex ?ecli ?case_no ?title
    WHERE {
        ?case cdm:resource_legal_id_celex ?celex .
        FILTER(regex(str(?celex), "[0-9]$"))
        ?case cdm:case-law_ecli ?ecli .
        OPTIONAL {?belonging_case cdm:dossier_contains_work ?case.}
        OPTIONAL {?belonging_case cdm:case_court_domain ?domain.}
        OPTIONAL {?belonging_case cdm:case_court_identifier_court "C"^^<http://www.w3.org/2001/XMLSchema#string> .}
        optional {?belonging_case cdm:dossier_identifier ?case_no. }
        ?e cdm:expression_belongs_to_work ?case.
        ?e cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG>.
        ?e cdm:expression_title ?title.
}
order by ?celex
"""

# Fetch all identifiers from the SPARQL endpoint and save them in a CSV file for faster lookup
def fetch_data():
    sparql = SPARQLWrapper("https://publications.europa.eu/webapi/rdf/sparql")
    sparql.setQuery(QUERY)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    data = []
    for result in results["results"]["bindings"]:
        data.append({
            "CELEX": result.get("celex", {}).get("value", ""),
            "ECLI": result.get("ecli", {}).get("value", ""),
            "CASE_NO": result.get("case_no", {}).get("value", ""),
            "TITLE": result.get("title", {}).get("value", ""),
        })
    return data

def add_missing_case_numbers(data):
    for item in data:
        if item["CASE_NO"] == "":
            # Try to find the case number from the ECLI
            ecli = item["ECLI"]
            if ecli:
                case_no = search_case_no(ecli)
                item["CASE_NO"] = case_no


if __name__ == "__main__":
    data = fetch_data()
    add_missing_case_numbers(data)
    # Save to CSV
    with open(CACHE_CSV, "w") as f:
        f.write("CELEX|ECLI|CASE_NO|TITLE\n")
        for item in data:
            f.write(f"{item['CELEX']}|{item['ECLI']}|{item['CASE_NO']}|{item['TITLE']}\n")
    