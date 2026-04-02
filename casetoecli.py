from bs4 import BeautifulSoup as bs
from lxml import etree as ET
from platformdirs import user_cache_dir
import argparse
import os
import pandas as pd
import re
import requests
import requests_cache


parser = argparse.ArgumentParser(description="Transform Case No to ECLI.")
parser.add_argument("--case", action="store", help="Case to search for")
parser.add_argument("--file", action="store", help="bulk file search")


CACHE_CSV = os.path.join("c2e.csv")
# parser = ET.HTMLParser()
requests_cache.install_cache(
    "eur_lex_cache",
    expire_after=-1,
    allowable_methods=["GET", "POST"],
    allowable_codes=[200, 301, 302, 303, 307],
)


def search(case_no):
    els = case_no.split("/")
    if len(els[1]) == 4:
        case_no = els[0] + "/" + els[1][2:]
    # data = pd.read_csv(CACHE_CSV, sep="|", na_filter=False)
    data = DATABASE
    try:
        # result = data.query(f'CASE_NO == "{case_no}"')
        result = data[data["CASE_NO"] == case_no].sort_values(by=["NAME"])
        if result["ECLI"].values[0] == "":
            info = result["NAME"].values[0]
            if ", see" in info:
                try:
                    new_case = re.search(r"see.*?C.(\d+\/\d+)", info).group(1)
                except:
                    # print(info)
                    new_case = re.search(r"see.*?Case\s+(\d+\/\d+)", info).group(1)
                    # print(new_case)
                # print(new_case)
                return search(new_case)
        return (result["ECLI"].values[0], result["NAME"].values[0])
    except IndexError as e:
        # print(case_no)
        return None, None


def search_title(ecli):
    data = DATABASE
    try:
        result = data[data["ECLI"] == ecli].sort_values(by=["NAME"])
        return result["NAME"].values[0]
    except IndexError as e:
        # print(case_no)
        return None


def search_case_no(ecli):
    data = DATABASE
    try:
        result = data[data["ECLI"] == ecli].sort_values(by=["NAME"])
        return result["CASE_NO"].values[0]
    except IndexError as e:
        # print(case_no)
        return None


def search_celex(identifier, session=None):
    if session is None:
        session = requests.Session()
    name = None
    if not identifier.startswith("ECLI"):
        identifier, name = search(identifier)
        if identifier is None:
            return None, None
    # ecli = parse.quote(ecli)
    response = session.get(
        f"https://publications.europa.eu/webapi/rdf/sparql?default-graph-uri=&query=prefix+cdm%3A+%3Chttp%3A%2F%2Fpublications.europa.eu%2Fontology%2Fcdm%23%3E+select+*+where+%7B%3Fs+cdm%3Acase-law_ecli+%3Fecli.%0D%0A%0D%0AFILTER%28str%28%3Fecli%29%3D%27{identifier}%27%29%0D%0A%0D%0A%3Fs+cdm%3Aresource_legal_id_celex+%3Fcelex%7D&format=application%2Fsparql-results%2Bjson&timeout=0&debug=on&run=+Run+Query+"
    )
    celex = response.json()["results"]["bindings"][0]["celex"]["value"]
    try:
        return celex, name
    except:
        return None, None


def search_url(ecli):
    celex = search_celex(ecli)[0]
    url = f"http://publications.europa.eu/resource/celex/{celex}.ENG.fmx4.ECR_{celex}_EN_01.xml"
    if requests.get(url).status_code == 200:
        return url
    return None


def search_celex_2(case_no, session=None):
    case_no, year = case_no.split("/")
    if len(year) == 2:
        if int(year[0]) >= 5:
            year = "19" + year
        else:
            year = "20" + year
    params = {
        "DTA": year,
        "SUBDOM_INIT": "ALL_ALL",
        "DB_TYPE_OF_ACT": "",
        "DTS_SUBDOM": "ALL_ALL",
        "typeOfActStatus": "OTHER",
        "DTS_DOM": "ALL",
        "FM_CODED": "JUDG",
        "lang": "en",
        "type": "advanced",
        "qid": "1658494303616",
        "DTN": case_no.zfill(4),
    }
    # print(case_no, year)

    if session is None:
        session = requests.Session()
    response = session.get("https://eur-lex.europa.eu/search.html", params=params)
    results = ET.HTML(response.text).findall(".//div[@class='SearchResult']")
    if len(results) == 1:
        return results[0].findall(".//dl/dd")[0].text, "".join(
            results[0].findall(".//a")[0].itertext()
        )
    return None, None


def update():
    urls = [
        "https://curia.europa.eu/en/content/juris/c1_juris.htm",
        "https://curia.europa.eu/en/content/juris/c2_juris.htm",
    ]
    print(CACHE_CSV)

    if not os.path.exists(CACHE_CSV):
        f = open(CACHE_CSV, "a")
        f.write("CASE_NO|ECLI|NAME\n")
    else:
        f = open(CACHE_CSV, "a")

    session = requests.Session()
    # celex_session = requests.Session()

    for url in urls:
        r = session.get(url)
        b = bs(r.content, "html.parser")

        table = b.find_all("table")

        for item in table:
            for row in item.find_all("tr"):
                try:
                    case_id = row.find_all("a")[0]["name"].replace("C-", "")
                    info, ecli = (row.find_all("i")[0].text.split("ECLI:") + [""])[:2]
                    if ", see" in ecli:
                        ecli = ecli.split(", see")[0]
                    if ecli != "":
                        ecli = "ECLI:" + ecli
                    # celex = search_celex(case_id, celex_session)
                    # if celex is None:
                    #     celex = ""
                    f.write(case_id + "|" + ecli + "|" + info.rstrip().lstrip() + "\n")
                    # result.append([item.find_all("a")[0], "ECLI:" + ecli if ecli is not None else None, info])

                except IndexError:
                    continue

    f.close()


if not os.path.exists(CACHE_CSV):
    update()
DATABASE = pd.read_csv(CACHE_CSV, sep="|", na_filter=False)
