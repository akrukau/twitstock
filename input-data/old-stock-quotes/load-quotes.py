from cqlengine import connection
from cqlengine.connection import get_session
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sys

conf = SparkConf().setAppName("StocksData").set("spark.cores.max", "100")
spark_context = SparkContext(conf=conf) 
sqlContext = SQLContext(spark_context) 

path = "~/sp500-may-2015.json"
df = sqlContext.read.json(path)
df.registerTempTable("StockQuotes")




#df.printSchema()

