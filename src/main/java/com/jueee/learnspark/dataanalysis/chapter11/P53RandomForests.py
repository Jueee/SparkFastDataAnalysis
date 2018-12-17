from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
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
numTrees = 3
featureSubsetStrategy = "auto"
impurity = "gini"
maxDepth = 5
maxBins = 32
seed = 5
decisionTreeModel = RandomForest.trainClassifier(points, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)
print("随机森林模型：" + decisionTreeModel.toDebugString())

# 未处理数据，带入模型处理
denseVec = Vectors.dense(3.5, 2.0, 3.0)
predict = decisionTreeModel.predict(denseVec)
print("predict: %s\n" % predict)

"""
随机森林模型：TreeEnsembleModel classifier with 3 trees

  Tree 0:
    Predict: 1.0
  Tree 1:
    If (feature 1 <= 2.5)
     Predict: 0.0
    Else (feature 1 > 2.5)
     Predict: 1.0
  Tree 2:
    Predict: 0.0

predict: 0.0
"""