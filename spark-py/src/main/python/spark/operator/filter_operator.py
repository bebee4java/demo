from pyspark import SparkContext
import time

sc = SparkContext(master="local", appName="filterOperator")
list_ = [1, 4, 6, 9, 10, 34, 23]
rdd = sc.parallelize(list_)


def filter(num):
    return num % 2 == 0


result = rdd.filter(filter).collect()
for num in result:
    time.sleep(10)
    print(num)
