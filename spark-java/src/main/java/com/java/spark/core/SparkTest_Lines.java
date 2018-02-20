package com.java.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * Created by sgr on 2017/12/13.
 */
public class SparkTest_Lines {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTest_Lines");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD rdd = sc.textFile("spark/src/data_160w.csv");
        rdd = rdd.cache();
        //没做持久化
        //cost: 5402    661     908

        //使用Memory_only持久化
        //cost: 5229    767     268

        //当RDD被复用的时候通常需要持久化
        //持久化默认策略为memory_only
        //如果内存吃紧可以使用memory_only_ser(序列化)
        //如果数据需要一定的容错可以使用_2(副本)
        //如果之间结果RDD计算代价大，可以选择memory_and_disk
        //memory_only是内存存不下就不存了
        //memory_and_disk如果内存存不下会存在本地磁盘，但尽量往内存存
//        rdd = rdd.persist(StorageLevel.MEMORY_AND_DISK());    //自定义缓存策略



        long start1 = System.currentTimeMillis();
        long count = rdd.count( );
        System.out.println(count);
        long end1 = System.currentTimeMillis();
        System.out.println("cost: " + (end1 - start1));

        long start2 = System.currentTimeMillis();
        long count2 = rdd.count( );
        System.out.println(count2);
        long end2 = System.currentTimeMillis();
        System.out.println("cost: " + (end2 - start2));

        long start3 = System.currentTimeMillis();
        long count3 = rdd.count( );
        System.out.println(count3);
        long end3 = System.currentTimeMillis();
        System.out.println("cost: " + (end3 - start3));

        sc.close();
    }
}
