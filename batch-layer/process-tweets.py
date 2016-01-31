from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
import json
import calendar
import time
import sys

def get_unix_time(ts_string):
    return calendar.timegm(time.strptime(ts_string,'%a %b %d %H:%M:%S +0000 %Y'))

def parse_tweet(line):
    tweet = json.loads(line)
    stock_symbol = []

    if "entities" in tweet and "symbols" in tweet["entities"] and "created_at" in tweet: 
        for entry in tweet['entities']['symbols']:     
            if "text" in entry and "text" in tweet:
                unix_time = get_unix_time(tweet["created_at"])
                stock_symbol.append( (unix_time, entry["text"], tweet["text"]) )
                
    return stock_symbol            

def send_to_cassandra(item, session):
    session.execute('INSERT INTO tweet_tags (date, tag, tweet_text) VALUES (%s, %s, %s)', \
            (item[0], item[1], item[2])) 

configuration = SparkConf().setAppName("TweetsData")
spark_context = SparkContext(conf = configuration)
# path = "/camus/topics/tweets1/hourly/2016/01/27/23/tweets1.2.0.1289.1289.1453964400000.gz"
path = "../input-data/may-2015-stock-tweets.json"
tweets_rdd = spark_context.textFile(path)

info = tweets_rdd.flatMap(parse_tweet)
# Show debug information
for entry in info.take(100):
    print "Date is:",entry[0],"Text is:", entry[2].encode('utf-8')
from cassandra.cluster import Cluster
cluster = Cluster(['52.88.73.44', '52.34.140.102', '52.34.147.146', '52.88.87.17'])
session = cluster.connect('tweet_keyspace')
info.map(lambda x: send_to_cassandra(x, session))
for item in info.collect():
    session.execute('INSERT INTO tweet_tags (date, tag, tweet_text) VALUES (%s, %s, %s)', \
            (item[0], item[1], item[2])) 
session.shutdown()
cluster.shutdown()

