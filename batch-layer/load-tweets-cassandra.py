from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
import json
import time
import sys
import re

#
#   Loads only Tweet count by minute to Cassandra 
#

def get_cassandra_time(ts_string):
    """
    Converts from Twitter API time format to Cassandra format  
    """
    return time.strftime('%Y-%m-%d %H:%M:%S', (time.strptime(ts_string,'%a %b %d %H:%M:%S +0000 %Y')) )

def init_tickers():
    tickers_file = open("../input-data/list-tickers.txt","r") 
    tickers = {}
    for line in tickers_file:
        tickers[line.rstrip()] = 1
    return tickers

def parse_tweet(line, tickers):
    records = []
    try: 
        tweet = json.loads(line)

        if "text" in tweet and "created_at" in tweet and "user" in tweet:
            matches = re.findall( r"\$[A-Z]{1,4}", tweet["text"])
            for match in matches:
                ticker = match[1:]
                if ticker in tickers:
                    if "screen_name" in tweet["user"]:
                        author = tweet["user"]["screen_name"]
                    else:
                        author = ""
                    if "followers_count" in tweet["user"]:
                        n_followers = tweet["user"]["followers_count"]
                    else:    
                        n_followers_count = "0"

                    cassandra_time = get_cassandra_time(tweet["created_at"])
                    records.append( (ticker, cassandra_time, author, n_followers, tweet["text"]) )
    
    except Exception as error:
        sys.stdout.write("Error trying to process the line\n")
        pass

    return records           



def get_minute(tweet):
    """ Rounds to the nearest minute """
    return ( (tweet[0], tweet[1][:16] + ':00'), 1)    

# Write to Cassandra
def load_part_cassandra(part):
    if part:
        from cassandra.cluster import Cluster
        # Original cluster
        # cluster = Cluster(['52.88.73.44', '52.34.140.102', '52.34.147.146', '52.88.87.17'])
        # Production cluster
        cluster = Cluster(['52.32.104.182', '52.32.248.128', '52.35.162.248', '52.35.228.210'])
        session = cluster.connect('tweet_keyspace')

        for entry in part:
            statement = "INSERT INTO tweets (ticker, time, n_tweets)"+ \
                "VALUES ('%s', '%s', %s)" % (entry[0][0], entry[0][1], entry[1])
            print "Statement", statement    
            session.execute(statement)
        
        session.shutdown()
        cluster.shutdown()

# Read and parse tweets
configuration = SparkConf().setAppName("TweetsData")
spark_context = SparkContext(conf = configuration)
#path = "../input-data/may-2015-stock-tweets.json"
path = "../input-data/saved.bz2"
tweets_rdd = spark_context.textFile(path)

# Show debug information
#for entry in tweets_rdd.collect():
#    print "Raw tweet is:",entry
tickers = init_tickers()
parsed_tweets = tweets_rdd.flatMap(lambda line: parse_tweet(line, tickers))

# Load to Cassandra
df_by_minute = parsed_tweets.map(get_minute).reduceByKey(lambda a, b: (a + b))
df_by_minute.foreachPartition(load_part_cassandra)

