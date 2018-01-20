from pyspark import SparkContext


if __name__ == '__main__':
    sc = SparkContext(master="local", appName="MapOperator")
    list_ = [1, 2, 3]
    rdd = sc.parallelize(list_)

    def my_map(n):
        return n * 10
    result = rdd.map(my_map).collect()
    for i in result:
        print(i)
