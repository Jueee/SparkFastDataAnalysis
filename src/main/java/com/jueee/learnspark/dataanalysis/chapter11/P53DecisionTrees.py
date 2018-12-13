from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.linalg import Vectors
from pyspark import SparkContext

sc = SparkContext('local')
denseVec1 = LabeledPoint(1.0, Vectors.dense([3.0,5.0,1.0]))
denseVec2 = LabeledPoint(0.0, Vectors.dense([2.0, 0.0, 1.0]))
vectors = [denseVec1, denseVec2]
points = sc.parallelize(vectors)
print(points)

# 设置决策树参数，训练模型
numClasses = 3
categoricalFeaturesInfo = {}
impurity = "gini"
maxDepth = 5
maxBins = 32
decisionTreeModel = DecisionTree.trainClassifier(points, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
print("决策树模型：" + decisionTreeModel.toDebugString())

# 未处理数据，带入模型处理
denseVec = Vectors.dense(3.5, 2.0, 3.0)
predict = decisionTreeModel.predict(denseVec)
print("predict: %s\n", predict)

"""
决策树模型：DecisionTreeModel classifier of depth 1 with 3 nodes
  If (feature 0 <= 2.5)
   Predict: 0.0
  Else (feature 0 > 2.5)
   Predict: 1.0

predict: %s
 1.0
"""