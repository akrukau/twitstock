from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
import json
import time
import sys
import re

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
    tweet = json.loads(line)
    records = []

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
    return records           

# Read and parse tweets
configuration = SparkConf().setAppName("TweetsData")
spark_context = SparkContext(conf = configuration)
# path = "/camus/topics/tweets1/hourly/2016/01/27/23/tweets1.2.0.1289.1289.1453964400000.gz"
path = "../input-data/may-2015-stock-tweets.json"
tweets_rdd = spark_context.textFile(path)
tickers = init_tickers()
parsed_tweets = tweets_rdd.flatMap(lambda line: parse_tweet(line, tickers))

# Show debug information
for entry in parsed_tweets.take(3):
    print "Date is:",entry[1],"Text is:", entry[4].encode('utf-8')
    print "Time is: %s" % entry[1]
# Load to Cassandra
from cassandra.cluster import Cluster
cluster = Cluster(['52.88.73.44', '52.34.140.102', '52.34.147.146', '52.88.87.17'])
session = cluster.connect('tweet_keyspace')
for item in parsed_tweets.collect():
    session.execute('INSERT INTO tweets (ticker, time, author, n_followers, tweet) VALUES (%s, %s, %s, %s, %s)',\
            (item[0], item[1], item[2], item[3], item[4])) 
session.shutdown()
cluster.shutdown()

