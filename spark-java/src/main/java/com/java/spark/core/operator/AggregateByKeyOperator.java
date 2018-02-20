package com.java.spark.core.operator;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sgr on 2018/2/5.
 */
public class AggregateByKeyOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("AggregateByKeyOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> list = Arrays.asList("hello world", "spark core", "spark world");
        JavaRDD<String> javaRDD = sparkContext.parallelize(list).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        JavaPairRDD<String,Integer> javaPairRDD = javaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });
        /**
         * aggregate算子和reduceByKey类似，其实reduceByKey是aggregate的一个特例
         * aggregate算子操作需要三个参数：
         * 1.参数一为每个key的初始值
         * 2.参数二为seq function即如何进行shuffle map-side 的本地聚合
         * 3.参数三位如何进行shuffle reduce-side 的全局聚合
         * */
        JavaPairRDD<String,Integer> result = javaPairRDD.aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        for (Tuple2<String,Integer> tuple2 : result.collect()){
            System.out.println(tuple2._1 + " -> " + tuple2._2);
        }
        sparkContext.close();
    }
}
