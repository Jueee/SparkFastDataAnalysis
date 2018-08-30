import csv
from io import StringIO
from pyspark import SparkContext



inputFile = r'E:\code\Java\workspace48\SparkFastDataAnalysis\src\main\resources\data\chapter05\FileFormats\demo.csv'

outputFile = r'E:\code\Java\workspace48\SparkFastDataAnalysis\src\main\resources\data\chapter05\FileFormats\save_by_phthon.csv'

sc = SparkContext('local')
print('sc:',sc)


# 在 Python 中使用 textFile() 读取 CSV
def loadRecord(line):
    input = StringIO(line)
    reader = csv.DictReader(input, fieldnames=["id","name","indate"])
    return reader.__next__()

fileData = sc.textFile(inputFile).map(loadRecord)
for value in fileData.collect():
    print(value,value['indate'])



# 在 Python 中完整读取 CSV
def loadRecords(fileNameContents):
    input = StringIO(fileNameContents[1])
    reader = csv.DictReader(input, fieldnames=["id","name","indate"])
    return reader

fullFileData = sc.wholeTextFiles(inputFile).flatMap(loadRecords)
for value in fullFileData.collect():
    print(value,value['name'])



# 在 Python 中写 CSV
def writeRecords(records):
    output = StringIO()
    write = csv.DictWriter(output, fieldnames=["id","name","indate"])
    for record in records:
        write.writerow(record)
    return [output.getvalue()]

fileData.mapPartitions(writeRecords).saveAsTextFile(outputFile)