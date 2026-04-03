export const PFX = `PREFIX cit:     <https://example.org/cjeu/citation#>
PREFIX dcterms: <http://purl.org/dc/terms/>

`;

export const SAVED_QUERIES = [
  {
    title: "Most-cited paragraphs",
    sparql: PFX +
      `SELECT ?cited ?paragraph ?text ?title (COUNT(?cit) AS ?times_cited)
WHERE {
  ?cit a cit:Citation ; cit:citesParagraph ?p .
  ?p cit:ofJudgment ?cited ; cit:paragraphNumber ?paragraph .
  OPTIONAL { ?p cit:text ?text }
  OPTIONAL { ?cited dcterms:title ?title }
}
GROUP BY ?cited ?paragraph ?text ?title
ORDER BY DESC(?times_cited)
LIMIT 50`,
  },
  {
    title: "Most-cited cases (distinct sources)",
    sparql: PFX +
      `SELECT ?cited ?title (COUNT(DISTINCT ?src) AS ?citing_cases)
WHERE {
  ?cit a cit:Citation ;
       cit:sourceJudgment ?src ;
       cit:citedJudgment ?cited .
  OPTIONAL { ?cited dcterms:title ?title }
}
GROUP BY ?cited ?title
ORDER BY DESC(?citing_cases)
LIMIT 50`,
  },
  {
    title: "Most-cited cases",
    sparql: PFX +
      `SELECT ?cited ?title
       (COUNT(?cit) AS ?total_citations)
       (COUNT(DISTINCT ?src) AS ?citing_cases)
WHERE {
  ?cit a cit:Citation ;
       cit:sourceJudgment ?src ;
       cit:citedJudgment ?cited .
  OPTIONAL { ?cited dcterms:title ?title }
}
GROUP BY ?cited ?title
ORDER BY DESC(?total_citations)
LIMIT 50`,
  },
  {
    title: "Most-citing source paragraphs",
    sparql: PFX +
      `SELECT ?src ?srcPara ?text (COUNT(?cit) AS ?outgoing)
WHERE {
  ?cit a cit:Citation ; cit:sourceParagraph ?sp .
  ?sp cit:ofJudgment ?src ; cit:paragraphNumber ?srcPara .
  OPTIONAL { ?sp cit:text ?text }
}
GROUP BY ?src ?srcPara ?text
ORDER BY DESC(?outgoing)
LIMIT 50`,
  },
  {
    title: "Citations by source case",
    sparql: PFX +
      `SELECT ?src (COUNT(?cit) AS ?citations_made)
WHERE {
  ?cit a cit:Citation ; cit:sourceJudgment ?src .
}
GROUP BY ?src
ORDER BY DESC(?citations_made)`,
  },
  {
    title: "Paragraph citation detail",
    sparql: PFX +
      `SELECT ?src ?srcPara ?srcTextCleaned ?cited ?citedPara ?citedTextCleaned
WHERE {
  ?cit a cit:Citation ;
       cit:sourceParagraph ?sp ;
       cit:citesParagraph ?tp .
  ?sp cit:ofJudgment ?src ; cit:paragraphNumber ?srcPara .
  ?tp cit:ofJudgment ?cited ; cit:paragraphNumber ?citedPara .
  OPTIONAL { ?sp cit:text ?srcText }
  OPTIONAL { ?tp cit:text ?citedText }
  BIND(COALESCE(?srcText, "") AS ?srcTextCleaned)
  BIND(COALESCE(?citedText, "") AS ?citedTextCleaned)
  FILTER(BOUND(?srcText) || BOUND(?citedText))
}
GROUP BY ?src ?srcPara ?srcTextCleaned ?cited ?citedPara ?citedTextCleaned
LIMIT 200`,
  },
];