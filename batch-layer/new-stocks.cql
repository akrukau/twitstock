--
-- Tweet table data
--

-- CREATE KEYSPACE tweet_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
use tweet_keyspace;

CREATE TABLE stocks (
    ticker text,
    time text,
    price float,
    volume int,
    PRIMARY KEY (ticker, time) 
) WITH CLUSTERING ORDER BY (time ASC);

