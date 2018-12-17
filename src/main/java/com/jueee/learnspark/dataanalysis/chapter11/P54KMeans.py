from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import KMeans
from pyspark.mllib.linalg import Vectors
from pyspark import SparkContext

sc = SparkContext('local')
vector1 = Vectors.dense([3.0,5.0,1.0])
vector2 = Vectors.dense([2.0, 0.0, 1.0])
vectors = [vector1, vector2]
points = sc.parallelize(vectors)
print(points)

# 设置参数，训练模型
numClusters = 3
numIterations = 20
kMeansModel = KMeans.train(points, numClusters, numIterations)

# 未处理数据，带入模型处理
denseVec = Vectors.dense(3.5, 2.0, 3.0)
predict = kMeansModel.predict(denseVec)
print("predict: %s\n" % predict)

"""
predict: 1
"""