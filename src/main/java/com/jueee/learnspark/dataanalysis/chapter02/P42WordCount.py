from pyspark import SparkConf,SparkContext
import os,re

inputFile = 'P42WordCount.py'
outputPath = r'E:\code\Java\workspace48\SparkFastDataAnalysis\src\main\resources\data\chapter02'

conf = SparkConf().setMaster("local").setAppName("MY App")
sc = SparkContext(conf = conf)
input = sc.textFile(inputFile)
words = input.flatMap(lambda x:re.split(r'[^a-zA-Z]',x))
count = words.map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y)
count.saveAsTextFile(outputPath + os.sep + "42wordcount" + os.sep + "wordcountByPython")
print('finish')