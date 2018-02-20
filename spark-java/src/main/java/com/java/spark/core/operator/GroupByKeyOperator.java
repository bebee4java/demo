package com.java.spark.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/4.
 */
public class GroupByKeyOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("GroupByKeyOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Tuple2<String,String>> list = Arrays.asList(
                new Tuple2<String, String>("zhangsan","lisi"),
                new Tuple2<String, String>("lisi","zhangsan"),
                new Tuple2<String, String>("lisi","wangwu"),
                new Tuple2<String, String>("wangwu","zhangsan")
        );
        JavaPairRDD<String,String> javaRDD = sparkContext.parallelizePairs(list);
        javaRDD.groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                System.out.println(tuple2._1 + " -> " + tuple2._2);
            }
        });

        sparkContext.close();
    }
}
