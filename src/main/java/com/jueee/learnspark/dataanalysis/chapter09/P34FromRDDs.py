from pyspark import SparkContext,Row
from pyspark.sql import HiveContext

sc = SparkContext('local')
hiveCtx = HiveContext(sc)
happyPeopleRDD = sc.parallelize([Row(name="holden", favouriteBeverage="coffee"),Row(name="holden2", favouriteBeverage="coffee2")])
happyPeopleSchemaRDD = hiveCtx._inferSchema(happyPeopleRDD)
happyPeopleSchemaRDD.fromInternal("happy_people")
print(happyPeopleSchemaRDD)
