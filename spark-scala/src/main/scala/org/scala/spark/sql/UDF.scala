package org.scala.spark.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 输入输出：N—>N
  * Created by sgr on 2018/3/3/003.
  */
object UDF {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("UDF").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val names = Array("zhangsan","lisi","wangwu","lilei")
    val rdd = sparkContext.parallelize(names,3)
    val rowRdd = rdd.map(name => Row(name))
    val schema = StructType(Array(StructField("name", StringType, true)))
    val namesDF = sqlContext.createDataFrame(rowRdd, schema)
    namesDF.createOrReplaceTempView("names")
    sqlContext.udf.register("strLength",(str : String) => str.length)
    val df = sqlContext.sql("select name,strLength(name) from names")
    df.rdd.foreach(println)
  }

}
