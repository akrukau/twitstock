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

# Read stock quotes
configuration = SparkConf().setAppName("StocksData")
spark_context = SparkContext(conf=configuration)
path = "./04-april-2013-stocks.json"
#path = "./sample.json"
from pyspark.sql import SQLContext
sqlContext = SQLContext(spark_context)
df = sqlContext.jsonFile("./sample.json")
df.show()

# Write to Cassandra
def aggToCassandraPart(agg):
    from cassandra.cluster import Cluster
    if agg:
        cluster = Cluster(['52.88.73.44', '52.34.140.102', '52.34.147.146', '52.88.87.17'])
        session = cluster.connect('tweet_keyspace')

        for quote in agg:
            print "Quote",quote[0],"type",type(quote)
            statement = "INSERT INTO stocks (ask_price, ask_volume, bid_price, bid_volume,"+ \
                "price, ticker, time, type, volume)"+ \
                "VALUES (%s, %s, %s, %s, %s, '%s', '%s', '%s', %s)" % quote
                #"VALUES (%s, %s, %s, %s, %s, '%s', '%s', '%s', %s)" % (quote[0], quote[1], quote[2], \
                # quote[3], quote[4], quote[5], quote[6], quote[7], quote[8])
            print statement
            session.execute(statement)
            #session.execute('INSERT INTO stocks (ask_price, ask_volume, bid_price, bid_volume,'+ \
                #     'price, ticker, time, type, volume)'+ \
                #'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)' % (quote[0], quote[1], quote[2], \
                #quote[3], quote[4], quote[5], quote[6], quote[7], quote[8]) )
                #(quote['ask_price'], \
                #quote['ask_volume'], quote['bid_price'], quote['bid_volume'], quote['price'], \
                #quote['ticker'], quote['time'], quote['type'], quote['volume']) )
        
        session.shutdown()
        cluster.shutdown()

df.foreachPartition(aggToCassandraPart)

