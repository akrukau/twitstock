#!/usr/bin/python
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel

# Read stock quotes
configuration = SparkConf().setAppName("StocksData")
spark_context = SparkContext(conf=configuration)
from pyspark.sql import SQLContext

sqlContext = SQLContext(spark_context)
#path = "../input-data/04-april-2013-stocks.json"
path = "../input-data/sample-stocks.json"
df = sqlContext.jsonFile(path)

# Write to Cassandra
def load_part_cassandra(part):
    from cassandra.cluster import Cluster
    if False:
        for quote in part:
            time_r = datetime.strptime(quote[7], "%Y-%m-%d %H:%M:%S")
            print "Hour:", time_r.hour
            print "Minute:", time_r.minute
    if part:
        cluster = Cluster(['52.88.73.44', '52.34.140.102', '52.34.147.146', '52.88.87.17'])
        session = cluster.connect('stock_keyspace')

        for quote in part:
            statement = "INSERT INTO stocks (ask_price, ask_volume, bid_price, bid_volume, id,"+ \
                "price, ticker, time, type, volume)"+ \
                "VALUES (%s, %s, %s, %s, %s, %s, '%s', '%s', '%s', %s)" % quote
            session.execute(statement)
        
        session.shutdown()
        cluster.shutdown()

df.foreachPartition(load_part_cassandra)
#df.show()
#for row in df_by_minute.collect():
#    print 'Row', row



#df2 = aggTSDelta(df, "ticker")
#df2 = df.withColumn('Hour', str(df.time))
#df3 = df2.withColumn('Minute', df2.time.cast("timestamp").minute)
#df3 = df2.withColumn('Minute', df2.time[14:16])
#df3 = df2.withColumn('Type', type(df2.time))
#df2.show()


