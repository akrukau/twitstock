CREATE KEYSPACE tweet_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
use tweet_keyspace;

CREATE TABLE sliding_window (
    date timestamp,
    return float,
    PRIMARY KEY (timestamp, tweet_keyspace) 
);
