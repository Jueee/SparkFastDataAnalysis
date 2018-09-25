from pyspark import SparkContext
import math


sc = SparkContext('local')
print('sc:',sc)
lines = sc.parallelize(["123","234","343"])
print(lines)

distanceNumerics = lines.map(lambda string: float(string))
stats = distanceNumerics.stats()
stddev = stats.stdev()
mean = stats.mean()
reasonableDistances = distanceNumerics.filter(
    lambda x: math.fabs(x - mean) < 3 * stddev)
print(reasonableDistances.collect())