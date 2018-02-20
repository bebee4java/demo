package com.java.spark.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/4.
 */
public class CollectOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("CollectOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> list = Arrays.asList("1", "2", "3", "4");
        JavaRDD<String> javaRDD = sparkContext.parallelize(list);

        /**
         * collect算子将分布式的远程集群里面的数据拉取到本地
         * 这种方式不建议使用，如果数据量大会有大量的网络传输甚至可能会出现OOM内存溢出
         * 通常会使用foreach算子：action操作，collect在远程集群上遍历RDD的元素
         */
        List<String> result = javaRDD.collect();
        for (String s : result){
            System.out.println(s);
        }

        javaRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sparkContext.close();
    }
}
