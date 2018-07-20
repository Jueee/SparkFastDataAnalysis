#coding=utf-8

import findspark;
findspark.init();

print('初始化SparkContext')

from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("MY App")
sc = SparkContext(conf = conf)
print(sc)