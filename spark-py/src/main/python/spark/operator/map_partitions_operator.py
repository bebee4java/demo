from pyspark import SparkContext

if __name__ == '__main__':
    sc = SparkContext(master="local", appName="mapPartitionsOperator")
    names = ["zhangsan", "lisi", "wangwu"]
    scores = {"zhangsan": 90, "lisi": 80, "wangwu": 95}
    rdd = sc.parallelize(names)

    def get_score(names):
        # list_ = []
        for name in names:
            # list_.append(scores[name])
            yield scores[name]
            # return list_

    result = rdd.mapPartitions(get_score).collect()
    for score in result:
        print(score)
