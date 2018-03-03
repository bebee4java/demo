package org.scala.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sgr on 2018/3/3/003.
  */
object PageRank {
  def main(args: Array[String]) {
    /*if (args.length < 1){
      System.err.println("Missing values for iters.")
      System.exit(-1)
    }*/
    val iters = 200
    val sparkConf = new SparkConf().setAppName("PageRank").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val lines = sparkContext.textFile("pages.txt", 2)

    sparkContext.setCheckpointDir(".")

    //根据边关系数据生成邻接表 如：(1,(2,3,4)) (2,(1,5))
    //这边需要重复使用我们做下cache
    val links = lines.map(line => {
      val parts = line.split("\\s+")
      (parts(0), parts(1))
    }).distinct().groupByKey().cache()

    //初始化每条表的rank值为1.0 如：(1,1.0) (2,1.0)
    //每条边的rank会发生改变 这里ranks集合需要用var声明
    var ranks = links.mapValues(v => 1.0)

    //进行迭代
    //样例：(1,((2,3,4),1.0))
    for (i <- 1 to iters){
      val result = links.join(ranks).values.flatMap{
        case (urls, rank) => {
          val size = urls.size
          urls.map(url => (url, rank / size))
        }
      }
      ranks = result.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      //为了解决迭代次数太多时 DAG关系图太大 程序跑不起来
      //在这之前先得在sparkContext上设值CheckpointDir
      ranks.checkpoint()
    }

    ranks.foreach(tup => println(tup._1 + " has rank is " + tup._2))
    //scala里对context可以不用关闭和stop 自动
    sparkContext.stop()
  }
}
