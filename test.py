import requests, zipfile, io

item = {
    "case": "http://publications.europa.eu/resource/cellar/fbe07f2e-bdec-11ef-91ed-01aa75ed71a1",
    "celex": "62023CJ0065",
    "ecli": "ECLI:EU:C:2024:1051",
    "belonging_case_identifier": "C-65/23",
    "domain": "EMPL",
    "title": "Judgment of the Court (Eighth Chamber) of 19 December 2024.#MK v K GmbH.#Request for a preliminary ruling from the Bundesarbeitsgericht.#Reference for a preliminary ruling \u2013 Protection of natural persons with regard to the processing of personal data \u2013 Regulation (EU) 2016/679 \u2013 Article 88(1) and (2) \u2013 Processing in the context of employment \u2013 Employees\u2019 personal data \u2013 More specific rules provided for by a Member State pursuant to that Article 88 \u2013 Obligation to comply with Article 5, Article 6(1) and Article 9(1) and (2) of that regulation \u2013 Processing on the basis of a collective agreement \u2013 Margin of discretion of the parties to the collective agreement as regards the necessity of the processing of personal data provided for by that agreement \u2013 Scope of judicial review.#Case C-65/23.",
    "legal_basis_cel": "32016R0679",
    "cited_work_cel": "32016R0679",
    "m": "http://publications.europa.eu/resource/cellar/fbe07f2e-bdec-11ef-91ed-01aa75ed71a1.0005.03",
    "url": "http://publications.europa.eu/resource/celex/62023CJ0065.ENG",
    "formex": "http://publications.europa.eu/resource/cellar/fbe07f2e-bdec-11ef-91ed-01aa75ed71a1.0005.03/DOC_1"
  }

response = requests.get(item["formex"], headers={"Accept": "*/*"}, allow_redirects=True)

with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    xml_content = z.read(z.namelist()[0]).decode("utf-8")

with open("output.xml", "w") as f:
    f.write(xml_content)

print(f"Saved {len(xml_content)} chars of Formex XML")