# 17 · Client compatibility

Slater speaks the Bolt protocol and presents itself to drivers as a modern Neo4j
server, so the Neo4j client ecosystem works against it. This page covers protocol
versions, tested clients, how a client selects a graph, and the introspection
commands clients expect to work.

## Bolt protocol

The server negotiates one of three Bolt versions, newest first:

| Version | Why |
|---|---|
| **5.4** | Preferred; the modern feature set. |
| **4.4** | Fallback for older drivers. |
| **4.1** | For the frozen `neo4rs` Rust driver. |

On connect the server reports the agent string **`Neo4j/5.4.0 (Slater
<version>)`**. The `Neo4j/`-prefix is deliberate: official drivers feature-gate
their behaviour on the server product string, and this makes them treat Slater as
a modern Bolt server. Slater is a large compatible *subset*, not a drop-in Neo4j
(see [01 Introduction](01-introduction.md)).

## Tested clients

- **Official Neo4j drivers** (Python, JavaScript, Java, Go, .NET) over Bolt 5.4 /
  4.4.
- **`neo4rs`** (Rust) over Bolt 4.1.
- **Memgraph Lab** and **Neo4j Browser** — Slater recognises the Neo4j- and
  Memgraph-dialect introspection queries these tools issue at startup and answers
  them from its manifest.

## Connecting and selecting a graph

Use a plain `bolt://` URL (not `neo4j://` — there is no routing/cluster layer),
authenticate with a user from the ACL, and select the graph with the driver's
`database` parameter:

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("admin", "yourpassword"))
with driver.session(database="social") as s:
    for r in s.run("MATCH (p:Person) RETURN p.name ORDER BY p.name"):
        print(r["p.name"])
```

You can also select the graph inside the query with a `USE` clause, e.g. `USE
social MATCH …`, which the server strips and applies before running the rest.

## Transactions

`BEGIN` / `COMMIT` / `ROLLBACK` are accepted, but a Bolt transaction on Slater is
a **read transaction** only — it carries no write-buffering semantics. Every write
statement is its own autocommit unit (one group-commit fsync); there is no
multi-statement write transaction. The ACL is re-checked on each `RUN`, not
trusted from `BEGIN`. See [11 Writing data](11-writing-data.md).

## Introspection commands

Clients (and dashboards) issue a range of `SHOW …` and `CALL …` introspection
commands to discover the server's capabilities and a graph's schema. Slater
answers these from its resident manifest — no graph scan. A selection:

- Server: `CALL dbms.components()`, `SHOW DATABASES`, `SHOW VERSION`, `SHOW
  PROCEDURES`, `SHOW FUNCTIONS`, `SHOW CONSTRAINTS`, `SHOW TRANSACTIONS`.
- Graph: `CALL db.labels()`, `CALL db.relationshipTypes()`, `CALL
  db.propertyKeys()`, `SHOW INDEXES` / `CALL db.indexes()`, `CALL db.meta.stats()`,
  the Memgraph `SHOW STORAGE INFO` / `SHOW INDEX INFO`.

Note that many of these are answered by the **server** before the query engine
sees them, so they work over a Bolt connection but not through the direct
`slater query` tool. The full list, their arguments, and their output columns are
in [09 Procedures & algorithms](09-procedures-and-algorithms.md).

## Next

- The procedures behind these commands: [09 Procedures & algorithms](09-procedures-and-algorithms.md).
- When a client reports an error: [18 Troubleshooting](18-troubleshooting.md).
