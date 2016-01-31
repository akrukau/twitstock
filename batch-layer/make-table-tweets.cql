--
-- Tweet table data
--

-- CREATE KEYSPACE tweet_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
use tweet_keyspace;

CREATE TABLE tweets (
    ticker text,
    time   timestamp,
    author text,
    tweet  text,
    PRIMARY KEY (ticker, time) 
) WITH CLUSTERING ORDER BY (time ASC);

