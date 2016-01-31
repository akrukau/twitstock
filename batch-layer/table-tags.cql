--CREATE KEYSPACE tweet_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
use tweet_keyspace;

CREATE TABLE tweet_tags (
    date timestamp,
    tag text,
    tweet_text text,
    PRIMARY KEY (tag, date) 
) WITH CLUSTERING ORDER BY (date ASC);

