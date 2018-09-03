
from pyspark import SparkContext



outputFile = r'E:\code\Java\workspace48\SparkFastDataAnalysis\target\classes\data\chapter05\FileFormats\save_by_scala.sequence\part-00000'

sc = SparkContext('local')
print('sc:',sc)

data = sc.sequenceFile(outputFile, "org.apache.hadoop.io.Text","org.apache.hadoop.io.IntWritable")


for value in data.collect():
    print(value)

