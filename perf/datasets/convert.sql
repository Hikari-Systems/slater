-- convert.sql — DuckDB → slater-build primitive-Cypher dump.
--
-- Reads the wikidata_csr.duckdb symmetric-CSR graph and emits a primitive-Cypher
-- creation script on /dev/stdout, in the order slater-build requires (ALL node
-- CREATEs, then ALL edge CREATEs). All the heavy lifting — the 1.5-billion-row
-- CSR expansion and string escaping — runs inside DuckDB's vectorised engine;
-- the wrapper (duckdb_to_slater.sh) only sets a few variables and routes stdout.
--
-- Parameters, set by the wrapper via `SET VARIABLE` before `.read`:
--   node_limit : 0 = full graph; N>0 = induced subgraph on csr_index < N
--   label      : node label  (e.g. 'Entity')   — slater-build labels are [A-Za-z0-9_]
--   reltype    : relationship type (e.g. 'LINK')
--   dedup      : 0 = faithful CSR (both directions; the graph is undirected so each
--                    edge is stored twice); 1 = keep only the src<dst direction,
--                    halving the symmetric duplication (drops self-loops; parallel
--                    multi-edges within the kept direction are preserved).
--
-- Source schema (CSR adjacency):
--   wikidata_mapping_node(csr_index, original_node_id)  dense id ↔ sparse wikidata id
--   wikidata_nodes_node(id, name)                       sparse id → label string
--   wikidata_indptr_rel(ptr)                            row k (rowid) = ptr[k], n_nodes+1 rows
--   wikidata_indices_rel(target)                        flat CSR column array; rowid = position
--   Node i's out-neighbours = indices_rel[ptr[i] : ptr[i+1]]. rowid is insertion-ordered
--   and equals the CSR index (verified monotonic, ptr[0]=0, ptr[last]=n_edges).

-- Nodes ----------------------------------------------------------------------
-- The __DumpVertex__ marker label + __dump_id__ property are the slater-build
-- linking convention (both stripped from the served graph). Names are escaped for
-- a single-quoted Cypher string: backslash first, then quote, then \n \r \t.
-- QUOTE '' + a never-occurring delimiter make COPY emit each row as a raw line.
COPY (
  SELECT 'CREATE (:' || getvariable('label') || ':__DumpVertex__ {__dump_id__: ' || m.csr_index
      || ', wikidata_id: ' || m.original_node_id
      || ', name: ' || CASE WHEN n.name IS NULL THEN 'null'
             ELSE '''' || replace(replace(replace(replace(replace(
                    n.name, '\', '\\'), '''', '\'''), chr(10), '\n'), chr(13), '\r'), chr(9), '\t') || ''''
           END
      || '});' AS line
  FROM wikidata_mapping_node m
  JOIN wikidata_nodes_node n ON m.original_node_id = n.id
  WHERE getvariable('node_limit') = 0 OR m.csr_index < getvariable('node_limit')
) TO '/dev/stdout' (FORMAT csv, HEADER false, QUOTE '', DELIMITER E'\x01');

-- Edges (CSR expansion) ------------------------------------------------------
-- ASOF JOIN maps each flat position `pos` to the source node whose ptr range
-- contains it (the greatest ptr <= pos). For a limited build we cap the inner
-- scan at ptr[node_limit] (sources < N) and keep only targets < N (induced).
COPY (
  SELECT 'MATCH (a:__DumpVertex__ {__dump_id__: ' || ip.csr
      || '}), (b:__DumpVertex__ {__dump_id__: ' || idx.target
      || '}) CREATE (a)-[:' || getvariable('reltype') || ']->(b);' AS line
  FROM (
    SELECT rowid AS pos, target
    FROM wikidata_indices_rel
    WHERE getvariable('node_limit') = 0
       OR ( rowid  < (SELECT ptr FROM wikidata_indptr_rel WHERE rowid = getvariable('node_limit'))
            AND target < getvariable('node_limit') )
  ) idx
  ASOF JOIN (SELECT rowid AS csr, ptr FROM wikidata_indptr_rel) ip
    ON idx.pos >= ip.ptr
  -- Parenthesised deliberately: `AND` binds tighter than `OR`, so dropping these
  -- makes the dedup disjunct swallow the cap and silently disable it.
  WHERE ( getvariable('dedup') = 0 OR idx.target > ip.csr )
  -- degree_cap: keep at most N out-edges per source (0 = uncapped).
  --
  -- Wikidata's degree distribution is a brutal power law. Over the full graph the
  -- max degree is 42,894,500, and 231 nodes (0.00025%) carry 11.9% of all 1.533B
  -- edges; the first-10M induced subgraph still peaks at 2,409,783. Those superhubs
  -- make a 2-hop expansion effectively unbounded — the worst-case frontier is
  -- degree², so one hub-adjacent anchor reaches tens of millions of rows and takes
  -- the machine with it. Capping bounds that at cap².
  --
  -- CSR adjacency is *contiguous per source*: node i owns flat positions
  -- [ptr[i], ptr[i+1]). So `pos - ptr[i]` already IS the neighbour's rank within
  -- i's adjacency — no window function needed. That matters: the obvious
  -- `QUALIFY row_number() OVER (PARTITION BY src ORDER BY pos)` is a sort over
  -- ~112M rows and OOMs DuckDB at a 4 GB limit. This form is O(1) per row and
  -- streams.
  --
  -- It caps the *raw* CSR degree, which upper-bounds the induced degree — the
  -- induced-subgraph filter above only ever removes more. The bound (and so the
  -- cap² frontier guarantee) holds either way; a hub whose first N raw neighbours
  -- largely fall outside the sample simply keeps fewer than N. Deterministic: the
  -- same cap always yields byte-identical output.
  --
  -- A capped fixture *understates* traversal cost, because the hubs are precisely
  -- what makes var-length expensive. Valid for A/B regression work (same graph in
  -- both arms); NOT a source for any absolute or competitive latency claim.
     AND ( getvariable('degree_cap') = 0
           OR idx.pos - ip.ptr < getvariable('degree_cap') )
) TO '/dev/stdout' (FORMAT csv, HEADER false, QUOTE '', DELIMITER E'\x01');

-- Enrichment overlay (overlay=1) ---------------------------------------------
-- NOT part of a faithful CSR dump — an *overlay* patch section that applies new
-- attributes via slater-build's MERGE/MATCH … SET dialect (overwrites nodes/edges
-- created above, matched by label + property). Emitted AFTER all CREATEs so the
-- targets exist. Gated on the `overlay` variable so the perf datasets are unchanged
-- by default; the inner constant predicate prunes the scan to empty when overlay=0.

-- (A) Node-property overwrite: attach out-degree to the top-200k highest-degree
--     nodes via MATCH … SET (exercises the (label,property)→node match index +
--     node-property fold at full-graph scale).
COPY (
  SELECT 'MATCH (n:' || getvariable('label') || ' {wikidata_id: ' || m.original_node_id
       || '}) SET n.degree = ' || d.deg || ';' AS line
  FROM (
    SELECT a.rowid AS csr, (b.ptr - a.ptr) AS deg
    FROM wikidata_indptr_rel a
    JOIN wikidata_indptr_rel b ON b.rowid = a.rowid + 1
    WHERE getvariable('overlay') = 1
      AND (getvariable('node_limit') = 0 OR a.rowid < getvariable('node_limit'))
    ORDER BY deg DESC
    LIMIT 200000
  ) d
  JOIN wikidata_mapping_node m ON m.csr_index = d.csr
) TO '/dev/stdout' (FORMAT csv, HEADER false, QUOTE '', DELIMITER E'\x01');

-- (B) Edge-property overwrite: set a weight on the first 200k CSR positions via
--     MATCH (a)-[r]->(b) SET (exercises endpoint match + edge fold; wikidata's
--     parallel duplicates make this match-all + last-writer-wins).
COPY (
  SELECT 'MATCH (a:' || getvariable('label') || ' {wikidata_id: ' || ma.original_node_id
       || '})-[r:' || getvariable('reltype') || ']->(b:' || getvariable('label')
       || ' {wikidata_id: ' || mb.original_node_id || '}) SET r.w = ' || idx.pos || ';' AS line
  FROM (
    SELECT rowid AS pos, target
    FROM wikidata_indices_rel
    WHERE getvariable('overlay') = 1
      AND rowid < 200000
      AND (getvariable('node_limit') = 0 OR target < getvariable('node_limit'))
  ) idx
  ASOF JOIN (SELECT rowid AS csr, ptr FROM wikidata_indptr_rel) ip ON idx.pos >= ip.ptr
  JOIN wikidata_mapping_node ma ON ma.csr_index = ip.csr
  JOIN wikidata_mapping_node mb ON mb.csr_index = idx.target
) TO '/dev/stdout' (FORMAT csv, HEADER false, QUOTE '', DELIMITER E'\x01');

-- (C) MERGE create-on-absent: 100 synthetic annotation nodes whose wikidata_id is
--     beyond any real id, so the MERGE matches nothing and creates the node.
COPY (
  SELECT 'MERGE (n:' || getvariable('label') || ' {wikidata_id: ' || (9000000000 + i)
       || '}) SET n.synthetic = true;' AS line
  FROM range(100) t(i)
  WHERE getvariable('overlay') = 1
) TO '/dev/stdout' (FORMAT csv, HEADER false, QUOTE '', DELIMITER E'\x01');
