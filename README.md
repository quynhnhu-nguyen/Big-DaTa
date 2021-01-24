import pyspark
from pyspark import SparkConf, SparkContext
import collections

conf= SparkConf().setMaster("local").setAppName("word counting")
sc= SparkContext.getOrCreate(conf= conf)

text_file = sc.textFile("/content/baitap.txt")
text = text_file.collect()

for i in text:
	print(i)

words = text_file.flatMap(lambda line: line.split(" "))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
print(wordCounts.collect())
max = wordCounts.reduce(lambda x, y : x if x[1] > y[1] else y)
result = ', '.join(map(str,[i[0] for i in wordCounts.collect() if(i[1] == max[1])]))

print("Các từ '" + result + "' xuất hiện nhiều nhất (tần suất "+ str(max[1]) + " lần)")# bigdata-bt1