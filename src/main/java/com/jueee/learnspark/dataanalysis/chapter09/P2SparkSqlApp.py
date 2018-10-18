from pyspark import SparkContext
from pyspark.sql import HiveContext,SQLContext

def initializingSparkSQL():
    print("[initializingSparkSQL]")
    sc = SparkContext('local')
    hiveCtx = HiveContext(sc)
    print(hiveCtx)
    return hiveCtx

def basicQueryExample(hiveCtx):
    input = hiveCtx.jsonFile(r"E:\code\Java\workspace48\SparkFastDataAnalysis\src\main\resources\data\chapter09\testweet.json")
    input.show()
    input.registerTempTable("tweets")
    result = hiveCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10")
    result.show()

hiveCtx = initializingSparkSQL()
basicQueryExample(hiveCtx)