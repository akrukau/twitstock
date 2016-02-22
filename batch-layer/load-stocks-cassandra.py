#!/usr/bin/python
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
import datetime
import pytz

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
            statement = "INSERT INTO stocks16 (ticker, time, price, volume)"+ \
                "VALUES ('%s', '%s', %s, %s)" % quote
            print "Statement", statement    
            session.execute(statement)
        
        session.shutdown()
        cluster.shutdown()

def get_timestamp(date_string, time_string):
    raw_time = datetime.datetime.strptime(date_string + " " + time_string, "%Y-%m-%d %H:%M:%S")
    # Use Polish timezone as the data is from Polish website
    localtimezone = pytz.timezone('Europe/Warsaw')
    local_time = localtimezone.localize(raw_time, is_dst=None)
    utc_time = local_time.astimezone(pytz.utc)
    return datetime.datetime.strftime(utc_time, '%Y-%m-%d %H:%M:%S')

def parse_stocks(line):
    fields = line.split(',')
    timestamp = get_timestamp(fields[1], fields[2])
    # fields[3] - open price 
    # fields[7] - volume of trading
    return (fields[0], timestamp, fields[3], fields[7])


# Read stock quotes
configuration = SparkConf().setAppName("StocksData")
spark_context = SparkContext(conf=configuration)

# Full data
path = "./sample-stocks-2016.csv"
df = spark_context.textFile(path)
parsed_stocks = df.map(parse_stocks)

#df_by_minute = df.map(get_by_minute).reduceByKey( \
#        lambda a, b: (a[0] + b[0], a[1] + b[1]) )

parsed_stocks.foreachPartition(load_part_cassandra)

