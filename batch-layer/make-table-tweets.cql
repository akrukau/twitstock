--
-- Tweet table data
--
-- DROP KEYSPACE IF EXISTS tweet_keyspace;
CREATE KEYSPACE tweet_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
use tweet_keyspace;

CREATE TABLE tweets (
    ticker text,
    time   timestamp,
    author text,
    n_followers int,
    n_tweets int,
    PRIMARY KEY (ticker, time) 
) WITH CLUSTERING ORDER BY (time ASC);

