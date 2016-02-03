#!/usr/bin/python
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel

# Read stock quotes
configuration = SparkConf().setAppName("StocksData")
spark_context = SparkContext(conf=configuration)
from pyspark.sql import SQLContext

sqlContext = SQLContext(spark_context)
path1 = "../input-data/stock-quotes/sample.json"
df = sqlContext.jsonFile(path1)

df.show()
path2 = "../input-data/may-2015-stock-tweets.json"
df = sqlContext.jsonFile(path2)
df_new = df.select()
df.show()

