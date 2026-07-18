# 02 · Quickstart

This page takes you from nothing to a served graph you can query, in five steps.
It uses the sample [`examples/social.cypher`](examples/) dump. You need the
`slater` and `slater-build` binaries (build them with `cargo build --release -p
slater -p slater-build`, or use a container image — see
[13 Deployment](13-deployment.md)).

## 1. Author a dump

A graph is described by a dump script. The default form is business-key `MERGE`
statements — one per node and per edge — where each node carries an inline
**business key** that identifies it. The sample dump starts by declaring range
indexes on those keys, then merges nodes and edges:

```cypher
CREATE INDEX FOR (n:Person) ON (n.email);
CREATE INDEX FOR (n:Company) ON (n.name);

MERGE (p:Person {email: 'ada@example.com'}) SET p.name = 'Ada Lovelace', p.age = 36;
MERGE (c:Company {name: 'Analytical Engines'}) SET c.founded = 1843;
MERGE (a:Person {email: 'ada@example.com'})-[r:WORKS_AT]->(b:Company {name: 'Analytical Engines'}) SET r.role = 'Analyst';
```

The full dump is in [`examples/social.cypher`](examples/social.cypher). See
[05 Building graphs](05-building-graphs.md) for every dump form.

## 2. Compile it

`slater-build` turns the dump into an immutable on-disk generation:

```sh
slater-build --input docs/manual/examples/social.cypher \
             --graph social --data-dir /tmp/slater-data
```

On success it prints the new generation UUID, the node/edge counts, and a content
hash:

```
built graph 'social' generation 7c6560e5-… (12 nodes, 17 edges)
content-hash 6a1f6cd0…
dir /tmp/slater-data/social/7c6560e5-…
```

## 3. Query it directly (no server)

For read queries and scripting you can skip the server entirely — `slater query`
opens the generation on disk and runs the query in-process. Point it at the data
directory and disable the ACL stamp (the sample graph is unstamped):

```sh
export dataBackend__fs__dir=/tmp/slater-data
export requireAclStamp=false

slater query social 'MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC LIMIT 3'
```

```json
{
  "columns": ["p.name", "p.age"],
  "rows": [
    ["Edsger Dijkstra", 52],
    ["Katherine Johnson", 48],
    ["Grace Hopper", 45]
  ]
}
```

> Configuration is supplied by a `config.json` and/or environment variables of the
> form `section__key` (double underscore nests). Here we override two settings
> inline. See [14 Configuration reference](14-configuration-reference.md).

## 4. Serve it over Bolt

To connect real drivers and tools — and to enable writes — run the server. It
needs an ACL file naming at least one user. Mint a password hash and write it in:

```sh
slater hash-password        # type a password; copy the $argon2id$… hash it prints
```

```json
// /tmp/slater-serve/acl.json
{
  "users": {
    "admin": {
      "passwordArgon2id": "$argon2id$v=19$m=19456,t=2,p=1$…",
      "grants": { "social": ["read", "write"] }
    }
  }
}
```

Start the server (again overriding config via environment variables):

```sh
export dataBackend__fs__dir=/tmp/slater-data
export requireAclStamp=false
export aclPath=/tmp/slater-serve/acl.json
export delta__enabled=true          # turn on the writable layer

slater
```

```
INFO starting slater (Bolt graph engine) version="0.24.1" port=7687 writable=true
INFO opened generation graph="social" nodes=12 edges=17 …
INFO slater Bolt listener ready bind=0.0.0.0 port=7687 …
```

## 5. Connect a driver

Any Neo4j driver works. Select the graph with the `database` parameter and use a
plain `bolt://` URL (no routing). With the Python driver:

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("admin", "yourpassword"))
with driver.session(database="social") as s:
    for r in s.run("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, f.name"):
        print(r.data())
```

Because the writable layer is on and the `admin` user has a `write` grant, you can
also mutate the graph — the write is visible to the next read immediately:

```python
with driver.session(database="social") as s:
    s.run("MERGE (p:Person {email:'linus@example.com'}) SET p.name='Linus Torvalds', p.age=54")
    print(s.run("MATCH (p:Person) RETURN count(*) AS people").single()["people"])   # 13
```

See [11 Writing data](11-writing-data.md) for the full write surface, and
[17 Client compatibility](17-client-compatibility.md) for driver notes.

## Where to go next

| You want to… | Read |
|---|---|
| Understand nodes, labels, and identity | [03 Data model](03-data-model.md) |
| Learn the query language | [07 Querying](07-querying.md) |
| Search embeddings | [10 Vector search](10-vector-search.md) |
| Insert / update / delete data | [11 Writing data](11-writing-data.md) |
| Configure and deploy the server | [13 Deployment](13-deployment.md) |
