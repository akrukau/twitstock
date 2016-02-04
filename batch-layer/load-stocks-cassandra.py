#!/usr/bin/python
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from datetime import datetime

# Read stock quotes
configuration = SparkConf().setAppName("StocksData")
spark_context = SparkContext(conf=configuration)
from pyspark.sql import SQLContext

sqlContext = SQLContext(spark_context)
#path = "../input-data/sample-stocks.json"
path = "../input-data/minute-stocks.json"
df = sqlContext.jsonFile(path)

# Write to Cassandra
def load_part_cassandra(part):
    if part:
        from cassandra.cluster import Cluster
        # Original cluster
        # cluster = Cluster(['52.88.73.44', '52.34.140.102', '52.34.147.146', '52.88.87.17'])
        # Production cluster
        cluster = Cluster(['52.32.104.182', '52.32.248.128', '52.35.162.248', '52.35.228.210'])
        session = cluster.connect('stock_keyspace')

        for quote in part:
            statement = "INSERT INTO stocks (ticker, time, price, volume, bid_ask)"+ \
                "VALUES ('%s', '%s', %s, %s, %s)" % quote
            print "Statement", statement    
            session.execute(statement)
        
        session.shutdown()
        cluster.shutdown()


def get_by_hour(quote):
    bid_ask = float(quote.ask_price) - float(quote.bid_price)
    return ((quote.ticker, quote.time[:13] + ':00:00'), (float(quote.price), \
        int(quote.volume), bid_ask, 1))    

def get_by_minute(quote):
    ask_price_num = float(quote.ask_price)
    bid_price_num = float(quote.bid_price)
    cutoff = 0.0000001
    # In the rare case there is no bid or ask offers,
    # set the spread to zero.
    if abs(ask_price_num) > cutoff and abs(bid_price_num) > cutoff:      
        bid_ask = ask_price_num - bid_price_num
    else:    
        bid_ask = 0.0
    return ((quote.ticker, quote.time[:16] + ':00'), (float(quote.price), \
        int(quote.volume), bid_ask, 1))    

def get_averages(quote):
    # Calculate averages for price and bid_ask spread, merge key and value
    # into a single tuple
    return (quote[0][0], quote[0][1], quote[1][0] / quote[1][3], quote[1][1], quote[1][2] / quote[1][3])    

df_by_minute = df.map(get_by_minute).reduceByKey( \
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3]) ).map(get_averages)

df_by_minute.foreachPartition(load_part_cassandra)

