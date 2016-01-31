from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
import json
import calendar
import time
import sys

def get_cassandra_time(ts_string):
    #return calendar.timegm(time.strptime(ts_string,'%a %b %d %H:%M:%S +0000 %Y'))
    return time.strftime('%Y-%m-%d %H:%M:%S', (time.strptime(ts_string,'%a %b %d %H:%M:%S +0000 %Y')) )

def parse_tweet(line):
    tweet = json.loads(line)
    records = []
    if "entities" in tweet and "symbols" in tweet["entities"] and "created_at" in tweet: 
        for entry in tweet['entities']['symbols']:     
            if "text" in entry and "text" in tweet and "user" in tweet \
                    and "screen_name" in tweet["user"]: 
                        cassandra_time = get_cassandra_time(tweet["created_at"])
                records.append( (entry["text"], cassandra_time, tweet["user"]["screen_name"], tweet["text"]) )
                
    return records           

configuration = SparkConf().setAppName("TweetsData")
spark_context = SparkContext(conf = configuration)
# path = "/camus/topics/tweets1/hourly/2016/01/27/23/tweets1.2.0.1289.1289.1453964400000.gz"
path = "../input-data/may-2015-stock-tweets.json"
tweets_rdd = spark_context.textFile(path)
parsed_tweets = tweets_rdd.flatMap(parse_tweet)

# Show debug information
for entry in parsed_tweets.take(3):
    print "Date is:",entry[1],"Text is:", entry[3].encode('utf-8')
    print "Time is: %s" % entry[1]
# Load to Cassandra
from cassandra.cluster import Cluster
cluster = Cluster(['52.88.73.44', '52.34.140.102', '52.34.147.146', '52.88.87.17'])
session = cluster.connect('tweet_keyspace')
for item in parsed_tweets.collect():
    session.execute('INSERT INTO tweets (ticker, time, author, tweet) VALUES (%s, %s, %s, %s)', \
            (item[0], item[1], item[2], item[3], item[4])) 
session.shutdown()
cluster.shutdown()

