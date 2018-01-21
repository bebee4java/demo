from pyspark import SparkContext

sc = SparkContext(master="local[2]", appName="mapPartitionsWithIndex")
list_ = ["zhangsan", "lisi", "wangwu"]
rdd = sc.parallelize(list_)


def call(index, names):
    for name in names:
        yield str(index) + ":" + name

result = rdd.mapPartitionsWithIndex(call).collect()
for s in result:
    print(s)
