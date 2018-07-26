
import findspark;
findspark.init();

from pyspark import SparkContext

# Python 行数统计
sc = SparkContext('local')
print('sc:',sc)
lines = sc.textFile('README.md')
pythonLines = lines.filter(lambda line: "Python" in line)
for line in pythonLines.take(pythonLines.count()):
    print(line)