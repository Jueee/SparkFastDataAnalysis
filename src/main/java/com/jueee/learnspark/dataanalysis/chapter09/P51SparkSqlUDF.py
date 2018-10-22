from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import IntegerType

def initializingSparkSQL():
    print("[initializingSparkSQL]")
    sc = SparkContext('local')
    hiveCtx = HiveContext(sc)
    print(hiveCtx)

    input = hiveCtx.read.json(r"E:\code\Java\workspace48\SparkFastDataAnalysis\src\main\resources\data\chapter09\testweet.json")
    input.show()
    input.registerTempTable("tweets")
    result = hiveCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10")
    result.show()

    hiveCtx.registerFunction("strLenPython", lambda x: len(x), IntegerType())
    lengthSchemaRDD = hiveCtx.sql("SELECT strLenPython(text),text FROM tweets LIMIT 10")
    lengthSchemaRDD.show()

initializingSparkSQL()