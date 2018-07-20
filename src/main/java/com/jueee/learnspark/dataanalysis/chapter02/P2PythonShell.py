#coding=utf-8

from pyspark import SparkContext

# Python 行数统计
sc = SparkContext('local')
print('sc:',sc)
lines = sc.textFile('README.md')
print('lines:',lines)