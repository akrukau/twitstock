--
-- Tweet table data
--

-- CREATE KEYSPACE tweet_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
use tweet_keyspace;

CREATE TABLE stocks (
    ticker text,
  --time timestamp,
    time text,
    type text,
    price float,
    volume int,
    ask_price float,
    ask_volume int,
    bid_price float,
    bid_volume int,
    PRIMARY KEY (ticker, time) 
) WITH CLUSTERING ORDER BY (time ASC);

