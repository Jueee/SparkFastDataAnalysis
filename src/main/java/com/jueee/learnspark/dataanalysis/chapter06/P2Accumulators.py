from pyspark import SparkContext

sc = SparkContext('local')
print('sc:',sc)
lines = sc.textFile(r'README.md')
print(lines)

# 创建Accumulator[Int]并初始化为0
blankLines = sc.accumulator(0)

def extractCallSigns(line):
    global blankLines
    if(line == ""):
        blankLines += 1
    return line.split(" ")

callSigns = lines.flatMap(extractCallSigns)
for value in callSigns.collect():
    print("[value]"+value)

print("Blank lines: %d" % blankLines.value)