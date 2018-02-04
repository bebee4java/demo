package com.java.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sgr on 2018/2/4.
 */
public class RePartitionOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("RePartitionOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> list = Arrays.asList("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8");
        JavaRDD javaRDD = sparkContext.parallelize(list, 3);

        JavaRDD<String> result = javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> tmep = new ArrayList<String>();
                while (iterator.hasNext()){
                    tmep.add("["+(index + 1)+"]" + iterator.next());
                }
                return tmep.iterator();
            }
        }, true);

        for (String s: result.collect()) {
            System.out.println(s);
        }

        /**
         * repartition算子，用于任意将RDD的partition增多或减少
         * coalesce算子往往是将RRD的partition减少（其实repartition算子还是调用coalesce方法）
         * 使用场景：
         * 有时候自动配置的partition数目过于少，为了进行优化可以增加partition数目提高并行度
         * 一个经典的例子：Spark SQL从hive查询数据，会根据hive对应的hdfs文件的block数目决定加载
         * 出来的RDD的partition数量，这里默认的partition数目是无法设置的但可以repartition
         */
        JavaRDD<String> javaRDD1 =  result.repartition(6);

        JavaRDD<String> result2 = javaRDD1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> temp = new ArrayList<String>();
                while (iterator.hasNext()){
                    temp.add("[" + (index+1) +"]" + iterator.next());
                }
                return temp.iterator();
            }
        },true);

        for (String s : result2.collect()){
            System.out.println(s);
        }

        sparkContext.close();
    }
}
