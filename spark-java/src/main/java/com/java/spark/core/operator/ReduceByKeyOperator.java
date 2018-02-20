package com.java.spark.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.collection.LinearSeq;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/5.
 */
public class ReduceByKeyOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("ReduceByKeyOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Tuple2<String,Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("zhangsan",100),
                new Tuple2<String, Integer>("lisi",90),
                new Tuple2<String, Integer>("wangwu", 99),
                new Tuple2<String, Integer>("zhangsan",90),
                new Tuple2<String, Integer>("wangwu",90)
        );
        JavaPairRDD<String,Integer> javaRDD = sparkContext.parallelizePairs(list);
        /**
         * reduceByKey算子 = groupByKey + reduce
         * groupByKey：shuffle 洗牌 = map + reduce
         * spark里的reduceByKey在map端自带Combiner
         */
        javaRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1 + " -> " + tuple2._2);
            }
        });
        sparkContext.close();
    }
}
