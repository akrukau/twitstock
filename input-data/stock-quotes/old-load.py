#!/usr/bin/python
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
import json
import sys
import csv
import re

def load_quote(line, session):
    session.execute("INSERT INTO stocks JSON\'" + line + "\'") 

# Read stock quotes
configuration = SparkConf().setAppName("StocksData")
spark_context = SparkContext(conf=configuration)
#path = "./04-april-2013-stocks.json"
path = "./sample.json"
quotes_rdd = spark_context.textFile(path)

# Load to Cassandra
from cassandra.cluster import Cluster
cluster = Cluster(['52.88.73.44', '52.34.140.102', '52.34.147.146', '52.88.87.17'])
session = cluster.connect('tweet_keyspace')
#for line in quotes_rdd.collect():
#    load_quote(line, session)

quotes_rdd.map(lambda quote: load_quote(quote, session))

#dicts_rdd = quotes_rdd.map(json.load)
#dicts_rdd.saveToCassandra("tweet_keyspace", "stocks")

session.shutdown()
cluster.shutdown()

#df.saveToCassandra("tweet_keyspace", "stocks"
# Load to Cassandra
#from cassandra.cluster import Cluster
#cluster = Cluster(['52.88.73.44', '52.34.140.102', '52.34.147.146', '52.88.87.17'])
#session = cluster.connect('tweet_keyspace')

#quotes_rdd.map(lambda line: json.loads)

#dicts_rdd = quotes_rdd.map(json.load)
#df.write.format("org.apache.spark.sql.cassandra").options(table="stocks", keyspace = "tweet_keyspace").save(mode ="append")

#session.shutdown()
#cluster.shutdown()

