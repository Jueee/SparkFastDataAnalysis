
from pyspark import SparkContext
from pyspark import StorageLevel

sc = SparkContext("local")

# 计算 RDD 中各值的平方
nums = sc.parallelize([1,2,3,4,5])
result = nums.map(lambda x:x*x)

result.persist(storageLevel=StorageLevel.DISK_ONLY)

print(result.count())
for value in result.collect():
    print(value)