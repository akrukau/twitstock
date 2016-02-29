from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
import datetime
import string
import json
import time
import sys
import re

#
#   Loads tweet data to Cassandra 
#

def get_cassandra_time(ts_string):
    """
    Converts from Twitter API time format to Cassandra format  
    """
    raw_time = datetime.datetime.strptime(ts_string,'%a %b %d %H:%M:%S +0000 %Y')
    return datetime.datetime.strftime(raw_time, '%Y-%m-%d %H:%M:%S')

def init_tickers():
    """
    Load tickers into a dictionary
    """
    tickers_file = open("../input-data/list-tickers.txt","r") 
    tickers = {}
    for line in tickers_file:
        tickers[line.rstrip()] = 1
    return tickers

def init_positive_words():
    pos_words_file = open("./textual-analysis/positive-words.txt","r") 
    pos_words = {}
    for line in pos_words_file:
        tokens = line.split()
        pos_words[line.split()[0]] = 1
    return pos_words    

def init_negative_words():
    neg_words_file = open("./textual-analysis/negative-words.txt","r") 
    neg_words = {}
    for line in neg_words_file:
        tokens = line.split()
        neg_words[line.split()[0]] = 1
    return neg_words
    
def get_sentiment(tweet_text, pos_words, neg_words):
    # Convert to ascii and lower case
    cleaned_text = re.sub("[\.\,\:\(\)\!\?]", "", tweet_text, 0, 0)
    words = cleaned_text.split()
    # Count of positive words
    pos_count = 0
    # Count of negative words
    neg_count = 0
    size = len(words)
    for i in xrange(size):
        word = words[i]
        if word in pos_words:
            pos_count += 1
        if word in neg_words: 
            neg_count += 1
    if pos_count + neg_count == 0:
        # Return 0, if no relevant words are found
        return  0.0
    else:    
        return -1.0 + 2.0 * pos_count / (pos_count + neg_count)


def parse_tweet(line, tickers, pos_words, neg_words):
    records = []
    try: 
        tweet = json.loads(line)

        if "text" in tweet and "created_at" in tweet and "user" in tweet:
            matches = re.findall( r"\$[A-Z]{1,4}", tweet["text"])
            #if True:
            for match in matches:
                ticker = match[1:]
                #if True:
                if ticker in tickers:
                    if "screen_name" in tweet["user"]:
                        author = tweet["user"]["screen_name"]
                    else:
                        author = ""
                    if "followers_count" in tweet["user"]:
                        n_followers = tweet["user"]["followers_count"]
                    else:    
                        n_followers_count = "0"

                    cassandra_time = get_cassandra_time(str(tweet["created_at"]))
                    # Convert to ASCII, and calculate sentiment
                    ascii_text = tweet["text"].encode('ascii','ignore').lower()
                    ascii_text = ascii_text.replace("'", "''")
                    sentiment = get_sentiment(ascii_text, pos_words, neg_words)
                    records.append( ((ticker, cassandra_time), \
                        (author, sentiment, ascii_text)) )
    
    except Exception as error:
        sys.stdout.write("Error trying to process the line: %s\n" % error)
        pass

    return records           

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
            statement = "INSERT INTO tweets_raw (ticker, time, author, sentiment, tweet_text)"+ \
                "VALUES ('%s', '%s', '%s', %s, '%s')" % (entry[0][0], entry[0][1], \
                    entry[1][0], entry[1][1], entry[1][2])
            session.execute(statement)
        
        session.shutdown()
        cluster.shutdown()

pos_words = init_positive_words()
neg_words = init_negative_words()
# Read and parse tweets
configuration = SparkConf().setAppName("TweetsData")
spark_context = SparkContext(conf = configuration)
# For HDFS, use name node DNS name.
#path = "../input-data/timo-data/small-tweets-2016.txt"
#path = "./one-day-stock-tweets.json"
#path = "../input-data/2016-02-08-11-57-tweets.txt"
path = "s3n://timo-twitter-data/2016-02-08-11-57_tweets.txt"
tweets_rdd = spark_context.textFile(path)

tickers = init_tickers()
parsed_tweets = tweets_rdd.flatMap(lambda line: parse_tweet(line, tickers, pos_words, neg_words))

# Print for debugging
#for tweet in parsed_tweets.collect():
#    print "Parsed tweet", tweet

# Load to Cassandra
#df_by_minute = parsed_tweets.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
parsed_tweets.foreachPartition(load_part_cassandra)

