#coding=utf-8

print('初始化SparkContext')

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

conf = SparkConf().setMaster("local").setAppName("MY App")
sc = SparkContext(conf = conf)
print(sc)