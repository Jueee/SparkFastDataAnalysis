from pyspark import SparkContext
from pyspark.sql import HiveContext

sc = SparkContext('local')
print('sc:',sc)

hiveCtx = HiveContext(sc)
rows = hiveCtx.jsonFile(r'E:\code\Java\workspace48\SparkFastDataAnalysis\src\main\resources\data\chapter05\FileFormats\json.txt')
rows.registerTempTable("test")
result = hiveCtx.sql("select date,city,data.ganmao from test")
firstRow = result.first()
print(firstRow.getString(0))