# Manual example dataset

Two small, self-contained graphs that every worked example in the manual runs
against. Both build in well under a second.

## `social.cypher` — the main graph

A tiny tech-company graph in the **business-key MERGE** dump format (Slater's
default import). It has three node labels and four relationship types:

- **Person** `{email (key), name, age, active, skills}` — 5 people
- **Company** `{name (key), founded, headcount, public}` — 3 companies
- **Product** `{sku (key), title, price}` — 4 products
- Relationships: `WORKS_AT`, `KNOWS`, `MAKES`, `USES` — 17 edges total

Each label has one **business key** (`Person.email`, `Company.name`,
`Product.sku`). The `CREATE INDEX` lines at the top make those keys
range-indexed, which is what lets range predicates and query-time writes target
them.

Build and query it:

```sh
# Compile the dump into an on-disk generation under /tmp/slater-data
slater-build --input social.cypher --graph social --data-dir /tmp/slater-data

# Run a read query directly against the generation (no server needed)
dataBackend__fs__dir=/tmp/slater-data requireAclStamp=false \
  slater query social 'MATCH (n) RETURN count(*) AS nodes'
# => 12
```

`slater query` opens the generation on disk and runs the query in-process — handy
for read queries and scripting. Server-only features (the `db.labels()` family of
introspection calls, and the writable layer) need a running `slater` server and a
Bolt client; see [02 Quickstart](../02-quickstart.md).

## `products-vec.cypher` — the vector graph

Four `Product` nodes carrying a 4-dimensional `embedding`, plus a cosine vector
index. It is written in the **CREATE / `--pk`** dump format because
**business-key MERGE dumps cannot carry vector values** — vectors enter a graph
either through the CREATE build form (shown here) or through the writable layer
at serve time (see [10 Vector search](../10-vector-search.md)).

```sh
slater-build --input products-vec.cypher --graph products \
  --data-dir /tmp/slater-data --pk __dump_id__

dataBackend__fs__dir=/tmp/slater-data requireAclStamp=false \
  slater query products \
  "CALL db.idx.vector.queryNodes('Product','embedding',3, vecf32([0.9,0.8,0.1,0.05])) \
   YIELD node, score RETURN node.sku, node.title, score"
```

## Notes

- The dump parser does **not** accept `//` or `/* */` comments — keep dump files
  to bare statements terminated by `;`.
- These graphs are unstamped (built without `--acl`), so a server serving them
  needs `requireAclStamp=false`. A production build stamps its ACL with
  `slater-build --acl acl.json`; see [15 Security](../15-security.md).
