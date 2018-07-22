#coding=utf-8
import findspark;
findspark.init();

from pyspark import SparkContext

# Python 行数统计
sc = SparkContext('local')
print('sc:',sc)
lines = sc.textFile('README.md')
print('lines:',lines)
pythonLines = lines.filter(lambda line: "Python" in line)
print(pythonLines.count())
print(pythonLines.first())

# 向 Spark 传递函数
def hasScala(line):
    return "Scala" in line
scalaLines = lines.filter(hasScala)
print(scalaLines.count())
print(scalaLines.first())