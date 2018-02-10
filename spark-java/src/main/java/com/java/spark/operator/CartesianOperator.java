package com.java.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/7/007.
 */
public class CartesianOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("CartesianOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> list1 = Arrays.asList("a","b","c","d");
        List<String> list2 = Arrays.asList("1","2","3","4");
        JavaRDD<String> javaRDD1 = sparkContext.parallelize(list1);
        JavaRDD<String> javaRDD2 = sparkContext.parallelize(list2);

        //笛卡尔集
        javaRDD1.cartesian(javaRDD2).foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });
        sparkContext.close();
    }
}
