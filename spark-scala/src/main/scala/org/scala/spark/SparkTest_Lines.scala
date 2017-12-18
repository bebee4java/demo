package org.scala.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sgr on 2017/12/17.
  */
object SparkTest_Lines {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkTest_Lines")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("spark/src/data_160w.csv")

    val count = rdd.count()
    println(count)

    val count1 = rdd.count()
    println(count1)
  }

}
