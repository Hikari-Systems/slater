CREATE INDEX FOR (n:Person) ON (n.email);
CREATE INDEX FOR (n:Company) ON (n.name);
CREATE INDEX FOR (n:Product) ON (n.sku);

MERGE (p:Person {email: 'ada@example.com'})   SET p.name = 'Ada Lovelace',   p.age = 36, p.active = true,  p.skills = ['math', 'analysis'];
MERGE (p:Person {email: 'alan@example.com'})  SET p.name = 'Alan Turing',    p.age = 41, p.active = true,  p.skills = ['logic', 'crypto'];
MERGE (p:Person {email: 'grace@example.com'}) SET p.name = 'Grace Hopper',   p.age = 45, p.active = true,  p.skills = ['compilers', 'navy'];
MERGE (p:Person {email: 'edsger@example.com'})SET p.name = 'Edsger Dijkstra',p.age = 52, p.active = false, p.skills = ['graphs', 'proofs'];
MERGE (p:Person {email: 'katherine@example.com'}) SET p.name = 'Katherine Johnson', p.age = 48, p.active = true, p.skills = ['orbital-mechanics'];

MERGE (c:Company {name: 'Analytical Engines'}) SET c.founded = 1843, c.headcount = 12,  c.public = false;
MERGE (c:Company {name: 'Bletchley Compute'})  SET c.founded = 1939, c.headcount = 240, c.public = false;
MERGE (c:Company {name: 'Remington Systems'})  SET c.founded = 1952, c.headcount = 900, c.public = true;

MERGE (p:Product {sku: 'ENG-1'}) SET p.title = 'Difference Engine',  p.price = 4500.0;
MERGE (p:Product {sku: 'BMB-1'}) SET p.title = 'Bombe',              p.price = 9900.0;
MERGE (p:Product {sku: 'CMP-1'}) SET p.title = 'A-0 Compiler',       p.price = 1200.0;
MERGE (p:Product {sku: 'NAV-1'}) SET p.title = 'Orbital Calculator', p.price = 3300.0;

MERGE (a:Person {email: 'ada@example.com'})-[r:WORKS_AT]->(b:Company {name: 'Analytical Engines'}) SET r.since = 1843, r.role = 'Analyst';
MERGE (a:Person {email: 'alan@example.com'})-[r:WORKS_AT]->(b:Company {name: 'Bletchley Compute'}) SET r.since = 1939, r.role = 'Cryptanalyst';
MERGE (a:Person {email: 'grace@example.com'})-[r:WORKS_AT]->(b:Company {name: 'Remington Systems'}) SET r.since = 1952, r.role = 'Engineer';
MERGE (a:Person {email: 'edsger@example.com'})-[r:WORKS_AT]->(b:Company {name: 'Remington Systems'}) SET r.since = 1962, r.role = 'Researcher';
MERGE (a:Person {email: 'katherine@example.com'})-[r:WORKS_AT]->(b:Company {name: 'Remington Systems'}) SET r.since = 1953, r.role = 'Mathematician';

MERGE (a:Person {email: 'ada@example.com'})-[r:KNOWS]->(b:Person {email: 'alan@example.com'})   SET r.since = 1936;
MERGE (a:Person {email: 'alan@example.com'})-[r:KNOWS]->(b:Person {email: 'grace@example.com'}) SET r.since = 1945;
MERGE (a:Person {email: 'grace@example.com'})-[r:KNOWS]->(b:Person {email: 'edsger@example.com'}) SET r.since = 1959;
MERGE (a:Person {email: 'edsger@example.com'})-[r:KNOWS]->(b:Person {email: 'katherine@example.com'}) SET r.since = 1960;

MERGE (a:Company {name: 'Analytical Engines'})-[r:MAKES]->(b:Product {sku: 'ENG-1'});
MERGE (a:Company {name: 'Bletchley Compute'})-[r:MAKES]->(b:Product {sku: 'BMB-1'});
MERGE (a:Company {name: 'Remington Systems'})-[r:MAKES]->(b:Product {sku: 'CMP-1'});
MERGE (a:Company {name: 'Remington Systems'})-[r:MAKES]->(b:Product {sku: 'NAV-1'});

MERGE (a:Person {email: 'ada@example.com'})-[r:USES]->(b:Product {sku: 'ENG-1'})   SET r.rating = 5;
MERGE (a:Person {email: 'alan@example.com'})-[r:USES]->(b:Product {sku: 'BMB-1'})  SET r.rating = 4;
MERGE (a:Person {email: 'grace@example.com'})-[r:USES]->(b:Product {sku: 'CMP-1'}) SET r.rating = 5;
MERGE (a:Person {email: 'katherine@example.com'})-[r:USES]->(b:Product {sku: 'NAV-1'}) SET r.rating = 3;
