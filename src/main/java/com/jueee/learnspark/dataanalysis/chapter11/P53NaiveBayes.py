from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.linalg import Vectors
from pyspark import SparkContext

sc = SparkContext('local')
denseVec1 = LabeledPoint(1.0, Vectors.dense([3.0,5.0,1.0]))
denseVec2 = LabeledPoint(0.0, Vectors.dense([2.0, 0.0, 1.0]))
vectors = [denseVec1, denseVec2]
dataset = sc.parallelize(vectors)
print(dataset)
model = NaiveBayes.train(dataset)
denseVec = Vectors.dense([1.5, 2.0, 3.0])
print("predict: %s, theta: %s\n" % (model.predict(denseVec), model.theta))

# predict: 0.0, theta: [[-0.69314718 -1.79175947 -1.09861229]
#  [-1.09861229 -0.69314718 -1.79175947]]