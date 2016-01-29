from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import json
import sys

configuration = SparkConf().setAppName("TweetsData").set("spark.cores.max", "100")
spark_context = SparkContext(conf=configuration) 
sqlContext = SQLContext(spark_context) 
path = "../input-data/may-2015-stock-tweets.json"
#parsed_tweets = raw_tweets.map(json.loads)

df = sqlContext.read.json(path)
df.flatMap(lambda p: p).collect()
df.registerTempTable("TweetsTable")
df.printSchema()
tag_count = sqlContext.sql("SELECT entities.symbols.text AS tags, COUNT(*) FROM TweetsTable WHERE \
    entities.symbols IS NOT NULL GROUP BY entities.symbols.text  ORDER BY COUNT(*) DESC")
for entry in tag_count.take(10):
    print entry


