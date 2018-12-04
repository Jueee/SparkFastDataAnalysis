from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.linalg import Vectors
from pyspark import SparkContext

# ImportError: cannot import name 'LogisticRegressionWithLBFGS' from 'pyspark.mllib.regression'

sc = SparkContext('local')
denseVec1 = LabeledPoint(1.0, Vectors.dense([-2.0,5.0,1.0]))
denseVec2 = LabeledPoint(0.0, Vectors.dense([2.0, 0.0, 1.0]))
vectors = [denseVec1, denseVec2]
dataset = sc.parallelize(vectors)
print(dataset)
model = LogisticRegressionWithLBFGS.train(dataset, iterations=200, intercept=True)
print("weights: %s, intercept: %s" % (model.weights, model.intercept))