from pyspark import SparkConf,SparkContext
import sys
import collections

if len(sys.argv)!=3:
    print("Usage:" + sys.argv[0] + "<in> <out>")
    sys.exit(1)
inputlocation=sys.argv[1]
outputlocation=sys.argv[2]

conf=SparkConf().setAppName("psfof")
sc=SparkContext(conf=conf)

## Data tansformation
data=sc.textFile(inputlocation)
data=data.map(lambda line: line.split())
data=data.map(lambda x: [int(i) for i in x])
dic=collections.defaultdict(list)
for i in data.collect(): dic[i[0]].extend(i[1:])
data=data.flatMap(lambda x: [[int(x[0]),int(i)] for i in dic[x[0]]])
data=data.map(lambda x: tuple(sorted(x))).distinct()
data=data.flatMap(lambda x: [x+(int(i),) for i in dic[x[1]] if ((x[0] in dic[i] or i in dic[x[0]]) and i>x[1])])


data.saveAsTextFile(outputlocation)
sc.stop()
