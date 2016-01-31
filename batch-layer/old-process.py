from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
import json
import sys

def parse_tweet(line):
    tweet = json.loads(line)
    stock_symbol = []

    if "entities" in tweet and "symbols" in tweet["entities"] and "created_at" in tweet: 
        for entry in tweet['entities']['symbols']:     
            if "text" in entry:
                stock_symbol.append( (entry["text"], tweet["created_at"]) )
                
    return stock_symbol            

def tweets_to_cassandra(items):
    from cqlengine import columns
    from cqlengine import connection
    from cqlengine.models import Model
    from cqlengine.management import sync_table
    from cqlengine.management import create_keyspace

    class TweetModel(Model):
        date = columns.Text(primary_key = True)    
        ticker = columns.Text()    

    host="localhost"
    connection.setup(['127.0.0.1'], "cqlengine")
    create_keyspace("cqlengine", "default_keyspace", 1)
    sync_table(TweetModel)
    for item in items:
        tweet_table.create(items)

    print "Number of elements in table:",TweetModel.objects.count()

configuration = SparkConf().setAppName("TweetsData").set("spark.cores.max", "100")
spark_context = SparkContext(conf = configuration)
# path = "/camus/topics/tweets1/hourly/2016/01/27/23/tweets1.2.0.1289.1289.1453964400000.gz"
path = "../input-data/may-2015-stock-tweets.json"
tweets_rdd = spark_context.textFile(path)

info = tweets_rdd.flatMap(parse_tweet)
# Show debug information
for entry in info.take(3):
    print entry

tweets_to_cassandra([])
info.foreachPartition(tweets_to_cassandra)


