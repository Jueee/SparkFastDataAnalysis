from pyspark import SparkContext
from pyspark.sql import HiveContext

sc = SparkContext('local')
print('sc:',sc)

hiveCtx = HiveContext(sc)
rows = hiveCtx.sql('select * from test limit 5')
firstRow = rows.first()
print(firstRow.getString(0))