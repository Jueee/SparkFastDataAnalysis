from pyspark import SparkContext

sc = SparkContext('local')
print('sc:',sc)
lines = sc.textFile(r'E:\code\Java\workspace48\SparkFastDataAnalysis\src\main\resources\data\chapter05\FileFormats\json.txt')
for line in lines.collect():
    print(line)

# 在 Python 中读取非结构化的 JSON
import json
data = lines.map(lambda x:json.loads(x))
for line in data.collect():
    print(line)

