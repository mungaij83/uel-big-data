import pyspark

sc=pyspark.SparkContext("local[*]")
def parseLogLine(line):
    line=line.replace('--',',')
    line=line.replace('.rb:',',')
    line=line.replace('/ghtorrent',',')
def getRDD():
    textfile=pyspack.textFile("hdfs://172.16.20.10:9000/ghtorrent-logs.txt",4)
    rdd=textfile.map(parseLogLine)
    return rdd
rowRDD=getRDD().cache()
print(rowRDD.count())
print(rowRDD.take(2))