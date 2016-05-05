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

CREATE TABLE IF NOT EXISTS stock.ohlc_1_min (
  symbol text,
  batch_time timestamp,
  open float,
  high float,
  low float,
  close float,
  PRIMARY KEY(symbol, batch_time)
)

CREATE TABLE IF NOT EXISTS stock.ohlc_1_hour (
  symbol text,
  batch_time timestamp,
  open float,
  high float,
  low float,
  close float,
  PRIMARY KEY(symbol, batch_time)
)

CREATE TABLE IF NOT EXISTS stock.ohlc_1_day (
  symbol text,
  batch_time timestamp,
  open float,
  high float,
  low float,
  close float,
  PRIMARY KEY(symbol, batch_time)
)
