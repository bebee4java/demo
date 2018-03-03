package org.scala.spark.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 输入输出：N—>M (M<=N) 聚合函数
  * Created by sgr on 2018/3/3/003.
  */
object UDAF {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("UDAF").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val names = Array("zhangsan","lisi","wangwu","lisi")
    val rdd = sparkContext.parallelize(names,3)
    val rowRdd = rdd.map(name => Row(name))
    val schema = StructType(Array(StructField("name", StringType, true)))
    val namesDF = sqlContext.createDataFrame(rowRdd, schema)
    namesDF.createOrReplaceTempView("names")
    sqlContext.udf.register("stringGroupCount", new StringGroupCount)
    val df = sqlContext.sql("select name,stringGroupCount(name) from names group by name")
    df.rdd.foreach(println)
  }

}
