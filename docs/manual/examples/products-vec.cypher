CREATE INDEX FOR (n:Product) ON (n.sku);

CALL db.idx.vector.createNodeIndex('Product', 'embedding', 4, 'cosine');

CREATE (:Product {__dump_id__: 0, sku: 'ENG-1', title: 'Difference Engine',  price: 4500.0, embedding: vecf32([0.10, 0.20, 0.30, 0.40])});
CREATE (:Product {__dump_id__: 1, sku: 'BMB-1', title: 'Bombe',              price: 9900.0, embedding: vecf32([0.20, 0.10, 0.40, 0.30])});
CREATE (:Product {__dump_id__: 2, sku: 'CMP-1', title: 'A-0 Compiler',       price: 1200.0, embedding: vecf32([0.90, 0.80, 0.10, 0.05])});
CREATE (:Product {__dump_id__: 3, sku: 'NAV-1', title: 'Orbital Calculator', price: 3300.0, embedding: vecf32([0.85, 0.75, 0.15, 0.10])});
