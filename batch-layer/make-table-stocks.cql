--
-- Stock table data
--
DROP KEYSPACE IF EXISTS stock_keyspace;
CREATE KEYSPACE stock_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
use stock_keyspace;

CREATE TABLE stocks (
    ticker text,
    time timestamp,
    type text,
    price float,
    volume int,
    ask_price float,
    ask_volume int,
    bid_price float,
    bid_volume int,
    id int,
    PRIMARY KEY (ticker, time) 
) WITH CLUSTERING ORDER BY (time ASC);

