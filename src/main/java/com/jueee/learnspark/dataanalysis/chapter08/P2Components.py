from pyspark import SparkContext

sc = SparkContext('local')
print('sc:',sc)
lines = sc.textFile(r'E:\code\Java\workspace48\SparkFastDataAnalysis\src\main\resources\data\chapter08\input.txt')

tokenized = lines.map(lambda line:line.split(" ")).filter(lambda words:len(words)>1)
counts = tokenized.map(lambda words:(words[0],1)).reduceByKey(lambda a,b:a+b)
print(lines.toDebugString())
print(counts.toDebugString())
for line in counts.collect():
    print(line)