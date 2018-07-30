
from pyspark import SparkContext

sc = SparkContext("local")

# 计算 RDD 中各值的平方
nums = sc.parallelize([1,2,3,4,5])
squared = nums.map(lambda x:x*x).collect()
for num in squared:
    print("%i" % num)

#  flatMap() 将行数据切分为单词
lines = sc.parallelize(["hello world","hi"])
words = lines.flatMap(lambda line:line.split(" "))
for value in words.collect():
    print(value)

# reduce() 计算元素的总和
sum = nums.reduce(lambda x,y:x+y)
print(sum)

# 用 aggregate() 来计算 RDD 的平均值
sumCount = nums.aggregate((0,0),
                          (lambda acc,value:(acc[0] + value, acc[1] + 1)),
                           (lambda acc1,acc2:(acc1[0] + acc2[0], acc1[1] + acc2[1])))
print(sumCount[0]/float(sumCount[1]))