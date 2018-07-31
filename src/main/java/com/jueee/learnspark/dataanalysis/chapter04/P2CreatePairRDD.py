from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("MY App")
sc = SparkContext(conf = conf)
lines = sc.textFile("README.md")
pairs = lines.map(lambda x:(x.split(" ")[0],x))
for value in pairs.collect():
    print(value)