from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
#file_tweets = open('../input-data/stock-tweets.json', "r")
#file_tweets = open('../input-data/oct-15-2011-00.txt', "r")
#file_tweets = open('../input-data/hello.json', "r")
file_tweets = open('../input-data/may-2015-stock-tweets.json', "r")

for tweet in file_tweets:
    #print "Tweet",tweet
    producer.send_messages("tweets1", tweet.rstrip())

