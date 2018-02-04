package com.java.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sgr on 2018/2/4.
 */
public class FlatMapOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("FlatMapOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> list = Arrays.asList("hello world", "spark core");
        JavaRDD<String> javaRDD = sparkContext.parallelize(list);

        //flat + map
        JavaRDD<String> words = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        words.foreach(new VoidFunction<String>() {
            @Override
            public void call(String word) throws Exception {
                System.out.println(word);
            }
        });
        sparkContext.close();
    }
}
