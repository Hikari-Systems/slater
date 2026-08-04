-- convert_monarch.sql — DuckDB → slater-build primitive-Cypher dump (`--pk __dump_id__`).
--
-- Reads the Monarch Knowledge Graph (monarch-kg.duckdb, Biolink model) and emits a
-- primitive-Cypher creation script on /dev/stdout, in the order slater-build's
-- single-global-key (`--pk __dump_id__`) import requires: ALL node CREATEs, then ALL
-- edge MATCH…CREATEs. All per-row work — the CURIE→dense-int mapping, string escaping
-- and list formatting — runs inside DuckDB's vectorised engine; the wrapper
-- (monarch_to_slater.sh) only frames the index DDL header / cleanup footer.
--
-- Why `--pk __dump_id__` (not default business-key MERGE): Monarch has 15.21M edges but
-- only 11.95M distinct (subject,predicate,object) — ~21% are parallel, provenance-distinct
-- edges (same pair+predicate, different publication/evidence). The CREATE path is a
-- multigraph and keeps them all; merge mode (edge identity = (src,reltype,dst)) would
-- collapse them. `--pk` needs an integer key, so we synthesise a dense `__dump_id__` per
-- node and keep the real CURIE as the stored `id` property.
--
-- Source schema used (other tables — dangling_edges, denormalized_*, closure, _solr_* — are
-- diagnostics and ignored):
--   nodes(id CURIE PK, category 'biolink:…', name, …, VARCHAR[] synonym/xref/…)   1,462,594 rows
--   edges(subject→id, object→id, predicate 'biolink:…', …, evidence_count BIGINT)  15,211,571 rows
-- The edges table is self-contained: every subject/object is present in nodes (0 dangling),
-- so no stub nodes are emitted.
--
-- Preamble (set by the wrapper before this script): the Monarch DuckDB is ATTACHed
-- read-only as catalog `src`, while the default catalog is in-memory — so the macros and
-- the `nmap` TEMP table below are writable even though the source file is read-only.
--   node_limit : 0 = full graph; N>0 = first N nodes (by id order) + the induced edges.

-- Escaping helpers ------------------------------------------------------------
-- A single-quoted Cypher string literal (NULL → SQL NULL so frag() omits the prop).
-- Backslash first, then quote, then \n \r \t — same order as convert.sql.
CREATE OR REPLACE MACRO cy_str(v) AS (
  CASE WHEN v IS NULL THEN NULL
  ELSE '''' || replace(replace(replace(replace(replace(
        v, '\', '\\'), '''', '\'''), chr(10), '\n'), chr(13), '\r'), chr(9), '\t') || ''''
  END
);

-- A Cypher list literal from a VARCHAR[] (NULL/empty → SQL NULL ⇒ prop omitted). The
-- per-element escape is inlined (same rules as cy_str) to avoid a macro call in the lambda.
CREATE OR REPLACE MACRO cy_list(a) AS (
  CASE WHEN a IS NULL OR len(a) = 0 THEN NULL
  ELSE '[' || array_to_string(list_transform(a, lambda x:
         '''' || replace(replace(replace(replace(replace(
           coalesce(x, ''), '\', '\\'), '''', '\'''), chr(10), '\n'), chr(13), '\r'), chr(9), '\t') || ''''
       ), ', ') || ']'
  END
);

-- A bare numeric literal (NULL → SQL NULL).
CREATE OR REPLACE MACRO cy_num(v) AS (CASE WHEN v IS NULL THEN NULL ELSE CAST(v AS VARCHAR) END);

-- A property fragment ", key: <formatted>" — emitted only when the value is non-null.
CREATE OR REPLACE MACRO frag(k, val) AS (CASE WHEN val IS NULL THEN '' ELSE ', ' || k || ': ' || val END);

-- Dense __dump_id__ for every node CURIE. id is unique & non-null, so this is 1:1 and the
-- ORDER BY makes it deterministic (the edge pass joins back to it). TEMP table lives in the
-- session's temp schema, so it is allowed even though the database is opened -readonly.
CREATE TEMP TABLE nmap AS
  SELECT id, CAST(row_number() OVER (ORDER BY id) - 1 AS BIGINT) AS did FROM src.nodes;

-- Nodes ----------------------------------------------------------------------
-- Label = category minus the `biolink:` prefix (all 18 are valid [A-Za-z0-9_]+). A common
-- `:Node` label (retained, indexable on `id`) + the `:__DumpVertex__` linking marker
-- (stripped by the footer along with __dump_id__) accompany it. All 26 columns are emitted;
-- null props are dropped per-row. QUOTE '' + a never-occurring delimiter ⇒ one raw line/row.
COPY (
  SELECT 'CREATE (:' || replace(n.category, 'biolink:', '') || ':Node:__DumpVertex__ {__dump_id__: ' || m.did
      || frag('id',                 cy_str(n.id))
      || frag('category',           cy_str(n.category))
      || frag('name',               cy_str(n.name))
      || frag('full_name',          cy_str(n.full_name))
      || frag('symbol',             cy_str(n.symbol))
      || frag('in_taxon',           cy_str(n.in_taxon))
      || frag('in_taxon_label',     cy_str(n.in_taxon_label))
      || frag('description',        cy_str(n.description))
      || frag('iri',                cy_str(n.iri))
      || frag('type',               cy_str(n."type"))
      || frag('namespace',          cy_str(n.namespace))
      || frag('provided_by',        cy_str(n.provided_by))
      || frag('file_source',        cy_str(n.file_source))
      || frag('deprecated',         cy_str(n.deprecated))
      || frag('has_biological_sex', cy_str(n.has_biological_sex))
      || frag('synonyms',           cy_str(n.synonyms))
      || frag('synonym',            cy_list(n.synonym))
      || frag('exact_synonym',      cy_list(n.exact_synonym))
      || frag('broad_synonym',      cy_list(n.broad_synonym))
      || frag('narrow_synonym',     cy_list(n.narrow_synonym))
      || frag('related_synonym',    cy_list(n.related_synonym))
      || frag('xref',               cy_list(n.xref))
      || frag('same_as',            cy_list(n.same_as))
      || frag('subsets',            cy_list(n.subsets))
      || frag('has_gene',           cy_list(n.has_gene))
      || frag('has_attribute',      cy_list(n.has_attribute))
      || '});' AS line
  FROM src.nodes n JOIN nmap m ON n.id = m.id
  WHERE getvariable('node_limit') = 0 OR m.did < getvariable('node_limit')
) TO '/dev/stdout' (FORMAT csv, HEADER false, QUOTE '', DELIMITER E'\x01');

-- Edges (multigraph: one CREATE per row) -------------------------------------
-- Endpoints referenced by __dump_id__; reltype = predicate minus `biolink:` (all 63 valid).
-- The edge's own uuid is kept as the `id` prop (always present ⇒ a non-empty {…} anchor);
-- the remaining 33 non-key columns follow as null-omitting fragments.
COPY (
  SELECT 'MATCH (a:__DumpVertex__ {__dump_id__: ' || sa.did
      || '}), (b:__DumpVertex__ {__dump_id__: ' || sb.did
      || '}) CREATE (a)-[:' || replace(e.predicate, 'biolink:', '')
      || ' {id: ' || cy_str(e.id)
      || frag('category',                        cy_str(e.category))
      || frag('agent_type',                      cy_str(e.agent_type))
      || frag('knowledge_level',                 cy_str(e.knowledge_level))
      || frag('primary_knowledge_source',        cy_str(e.primary_knowledge_source))
      || frag('provided_by',                     cy_str(e.provided_by))
      || frag('file_source',                     cy_str(e.file_source))
      || frag('object_category',                 cy_str(e.object_category))
      || frag('subject_category',                cy_str(e.subject_category))
      || frag('original_predicate',              cy_str(e.original_predicate))
      || frag('original_subject',                cy_str(e.original_subject))
      || frag('original_object',                 cy_str(e.original_object))
      || frag('negated',                         cy_str(e.negated))
      || frag('qualifier',                       cy_str(e.qualifier))
      || frag('object_specialization_qualifier', cy_str(e.object_specialization_qualifier))
      || frag('object_aspect_qualifier',         cy_str(e.object_aspect_qualifier))
      || frag('frequency_qualifier',             cy_str(e.frequency_qualifier))
      || frag('disease_context_qualifier',       cy_str(e.disease_context_qualifier))
      || frag('onset_qualifier',                 cy_str(e.onset_qualifier))
      || frag('sex_qualifier',                   cy_str(e.sex_qualifier))
      || frag('species_context_qualifier',       cy_str(e.species_context_qualifier))
      || frag('stage_qualifier',                 cy_str(e.stage_qualifier))
      || frag('FDA_adverse_event_level',         cy_str(e.FDA_adverse_event_level))
      || frag('has_count',                       cy_str(e.has_count))
      || frag('has_total',                       cy_str(e.has_total))
      || frag('has_percentage',                  cy_str(e.has_percentage))
      || frag('has_quotient',                    cy_str(e.has_quotient))
      || frag('grouping_key',                    cy_str(e.grouping_key))
      || frag('evidence_count',                  cy_num(e.evidence_count))
      || frag('aggregator_knowledge_source',     cy_list(e.aggregator_knowledge_source))
      || frag('publications',                    cy_list(e.publications))
      || frag('qualifiers',                      cy_list(e.qualifiers))
      || frag('has_evidence',                    cy_list(e.has_evidence))
      || frag('has_attribute',                   cy_list(e.has_attribute))
      || '}]->(b);' AS line
  FROM src.edges e
  JOIN nmap sa ON e.subject = sa.id
  JOIN nmap sb ON e.object  = sb.id
  WHERE getvariable('node_limit') = 0
     OR (sa.did < getvariable('node_limit') AND sb.did < getvariable('node_limit'))
) TO '/dev/stdout' (FORMAT csv, HEADER false, QUOTE '', DELIMITER E'\x01');
