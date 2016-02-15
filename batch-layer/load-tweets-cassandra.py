from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
import datetime
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
    Epoch seconds are rounded towards the nearest 5 minute interval.
    The reason is that available stock data is in 5 minute interval. 
    """
    #  Rounding interval, in minutes
    round_interval = 5
    raw_time = datetime.datetime.strptime(ts_string,'%a %b %d %H:%M:%S +0000 %Y')
    final_time = raw_time - datetime.timedelta(minutes=raw_time.minute % round_interval, \
        seconds=raw_time.second, microseconds=raw_time.microsecond)
    print "Timestamp", ts_string,"Raw time", datetime.datetime.strftime(raw_time, '%Y-%m-%d %H:%M:%S'), \
        "actual time", datetime.datetime.strftime(final_time, '%Y-%m-%d %H:%M:%S') 
    return datetime.datetime.strftime(final_time, '%Y-%m-%d %H:%M:%S')

def init_tickers():
    """
    Load tickers into a dictionary
    """
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
            if True:
            #for match in matches:
                #ticker = match[1:]
                if True:
                #if ticker in tickers:
                    if "screen_name" in tweet["user"]:
                        author = tweet["user"]["screen_name"]
                    else:
                        author = ""
                    if "followers_count" in tweet["user"]:
                        n_followers = tweet["user"]["followers_count"]
                    else:    
                        n_followers_count = "0"

                    cassandra_time = get_cassandra_time(str(tweet["created_at"]))
                    records.append( (ticker, cassandra_time, author, n_followers, tweet["text"]) )
    
    except Exception as error:
        sys.stdout.write("Error trying to process the line: %s\n" % error)
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
            statement = "INSERT INTO tweets16 (ticker, time, n_tweets)"+ \
                "VALUES ('%s', '%s', %s)" % (entry[0][0], entry[0][1], entry[1])
            print "Statement", statement    
            session.execute(statement)
        
        session.shutdown()
        cluster.shutdown()

# Read and parse tweets
configuration = SparkConf().setAppName("TweetsData")
spark_context = SparkContext(conf = configuration)
path = "../input-data/timo-data/small-tweets-2016.txt"

# For HDFS, use name node DNS name.
#path = "hdfs://ec2-52-34-147-146.us-west-2.compute.amazonaws.com:9000/tweets/full-tweets-2015.bz2"
tweets_rdd = spark_context.textFile(path)

# Show debug information
#for entry in tweets_rdd.collect():
#    print "Raw tweet is:",entry
tickers = init_tickers()
parsed_tweets = tweets_rdd.flatMap(lambda line: parse_tweet(line, tickers))
# Print for debugging
for tweet in parsed_tweets.collect():
    print "Parsed tweet", tweet
# Load to Cassandra
#df_by_minute = parsed_tweets.reduceByKey(lambda a, b: (a + b))
#df_by_minute.foreachPartition(load_part_cassandra)

