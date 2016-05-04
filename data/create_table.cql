CREATE KEYSPACE IF NOT EXISTS stock
WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS stock.transactions (
  symbol text,
  tx_time timestamp,
  price float,
  volume int,
  matched_by text,
  PRIMARY KEY (symbol, tx_time)
);
