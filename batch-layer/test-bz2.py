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

    if "text" in tweet and "created_at" in tweet: 
        unix_time = get_unix_time(tweet["created_at"])
        stock_symbol.append( (unix_time, tweet["text"], tweet["text"].encode('utf-8')) )
                
    return stock_symbol            

def send_to_cassandra(item, session):
    session.execute('INSERT INTO tweet_tags (date, tag, tweet_text) VALUES (%s, %s, %s)', \
            (item[0], item[1], item[2])) 

string = u'\u3066\u3044\u3046\u304b\u6c17\u6301\u3061\u60aa\u3044'
string = u'\u65e5\u6bd4\u91ce\u3055\u3093\u2026\u524d\u306e\u5e2d\u884c\u304b\u306a\u3044\uff1f'
print "Decoded string",string.encode('utf-8')

configuration = SparkConf().setAppName("TweetsData")
spark_context = SparkContext(conf = configuration)
# path = "/camus/topics/tweets1/hourly/2016/01/27/23/tweets1.2.0.1289.1289.1453964400000.gz"
path = "../input-data/01.json.bz2"
tweets_rdd = spark_context.textFile(path)

info = tweets_rdd.flatMap(parse_tweet)
# Show debug information
for entry in info.take(10):
    print "Entry is", entry
