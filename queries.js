export const PFX = `PREFIX cit:     <https://example.org/cjeu/citation#>
PREFIX eli:     <http://data.europa.eu/eli/ontology#>
PREFIX dcterms: <http://purl.org/dc/terms/>

`;

export const SAVED_QUERIES = [
  {
    title: "Most-cited paragraphs",
    sparql: PFX +
      `SELECT ?cited ?paragraph (COUNT(?src) AS ?times_cited)
WHERE {
  ?src cit:cites ?p .
  ?p a cit:Paragraph ; cit:ofJudgment ?cited ; cit:paragraphNumber ?paragraph .
}
GROUP BY ?cited ?paragraph
ORDER BY DESC(?times_cited)
LIMIT 50`,
  },
  {
    title: "Most-cited cases (distinct sources)",
    sparql: PFX +
      `SELECT ?cited (COUNT(DISTINCT ?srcJ) AS ?citing_cases)
WHERE {
  ?sp a cit:Paragraph ;
      cit:ofJudgment ?srcJ ;
      cit:cites ?tp .
  ?tp cit:ofJudgment ?cited .
  FILTER(?srcJ != ?cited)
}
GROUP BY ?cited
ORDER BY DESC(?citing_cases)
LIMIT 50`,
  },
  {
    title: "Most-cited cases",
    sparql: PFX +
      `SELECT ?cited
       (COUNT(?sp) AS ?total_citations)
       (COUNT(DISTINCT ?srcJ) AS ?citing_cases)
WHERE {
  ?sp a cit:Paragraph ;
      cit:ofJudgment ?srcJ ;
      cit:cites ?tp .
  ?tp cit:ofJudgment ?cited .
  FILTER(?srcJ != ?cited)
}
GROUP BY ?cited
ORDER BY DESC(?total_citations)
LIMIT 50`,
  },
  {
    title: "Most-citing source paragraphs",
    sparql: PFX +
      `SELECT ?srcJ ?srcPara (COUNT(?target) AS ?outgoing)
WHERE {
  ?sp a cit:Paragraph ;
      cit:ofJudgment ?srcJ ;
      cit:paragraphNumber ?srcPara ;
      cit:cites ?target .
}
GROUP BY ?srcJ ?srcPara
ORDER BY DESC(?outgoing)
LIMIT 50`,
  },
  {
    title: "Citations by source case",
    sparql: PFX +
      `SELECT ?srcJ (COUNT(?sp) AS ?citations_made)
WHERE {
  ?sp a cit:Paragraph ; cit:ofJudgment ?srcJ ; cit:cites ?target .
}
GROUP BY ?srcJ
ORDER BY DESC(?citations_made)`,
  },
  {
    title: "Paragraph citation detail",
    sparql: PFX +
      `SELECT ?srcJ ?srcPara ?citedJ ?citedPara
WHERE {
  ?sp a cit:Paragraph ;
      cit:ofJudgment ?srcJ ;
      cit:paragraphNumber ?srcPara ;
      cit:cites ?tp .
  ?tp a cit:Paragraph ;
      cit:ofJudgment ?citedJ ;
      cit:paragraphNumber ?citedPara .
}
LIMIT 50`,
  },
];