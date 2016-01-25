from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
file_tweets = open('../input-data/oct-15-2011-00.txt', "r")

for tweet in file_tweets:
    producer.send_messages("tweets", tweet)

