#!/usr/bin/python
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
import json
import sys
import os

def load_quote(line, session):
    session.execute("INSERT INTO stocks JSON\'" + line + "\'") 

# Read stock quotes
configuration = SparkConf().setAppName("StocksData")
spark_context = SparkContext(conf=configuration)
#path = "./04-april-2013-stocks.json"
path = "./sample.json"
quotes_rdd = spark_context.textFile(path)
from pyspark.sql import SQLContext
sqlContext = SQLContext(spark_context)
df = sqlContext.jsonFile("./sample.json")
df.show()

# Write to Cassandra
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table

class stocks(Model):
    ticker = columns.Text(primary_key=True)
    #time = columns.DateTime(primary_key=True)
    time = columns.Text(primary_key=True)
    #type = columns.Text
    price =  columns.Float
    volume = columns.Integer
    #ask_price =  columns.Float
    #ask_volume = columns.Integer
    #bid_price =  columns.Float
    #bid_volume = columns.Integer

connection.setup(['52.88.73.44', '52.34.140.102', '52.34.147.146', '52.88.87.17'], "tweet_keyspace")
sync_table(stocks)

for quote in df:
    try:
        stocks.create(ticker = quote['ticker'], price = quote['price'], volume = quote['volume'])
        #stocks.create(ticker = quote['ticker'], time = quote['time'], type = quote['type'], \
        #    price = quote['price'], volume = quote['volume'], ask_price = quote['ask_price'], \
        #    ask_volume = quote['ask_volume'], bid_price = quote['bid_price'], \
        #    bid_volume = quote['bid_volume'])
    except Exception as exception:
        print "Exception is:", exception
        #, "quote",quote


