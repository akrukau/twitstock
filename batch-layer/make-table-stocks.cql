--
-- Stock table data
--
--DROP KEYSPACE IF EXISTS stock_keyspace;
--CREATE KEYSPACE stock_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
use stock_keyspace;

CREATE TABLE stocks (
    ticker text,
    time timestamp,
    price float,
    volume int,
    bid_ask float,
    PRIMARY KEY (ticker, time) 
) WITH CLUSTERING ORDER BY (time ASC);

