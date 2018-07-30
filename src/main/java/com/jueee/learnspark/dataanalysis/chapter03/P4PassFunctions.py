from pyspark import SparkContext

def containsPython(s):
    return "Python" in s

# 在 Python 中传递函数
sc = SparkContext('local')
print('sc:',sc)
lines = sc.textFile('README.md')
pythonLines = lines.filter(containsPython)
for line in pythonLines.take(pythonLines.count()):
    print(line)


