package com.java.spark.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/9/009.
 */
public class CogroupOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("CogroupOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Tuple2<Integer,String>> nameList = Arrays.asList(
                new Tuple2<Integer, String>(1,"zhangsan"),
                new Tuple2<Integer, String>(2,"lisi"),
                new Tuple2<Integer, String>(3,"wangwu")
        );
        List<Tuple2<Integer,Integer>> socreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1,100),
                new Tuple2<Integer, Integer>(2,90),
                new Tuple2<Integer, Integer>(3,99),
                new Tuple2<Integer, Integer>(1,90),
                new Tuple2<Integer, Integer>(2,98),
                new Tuple2<Integer, Integer>(3,90)
        );
        JavaPairRDD<Integer,String> nameRdd = sparkContext.parallelizePairs(nameList);
        JavaPairRDD<Integer,Integer> socreRdd = sparkContext.parallelizePairs(socreList);
        nameRdd.cogroup(socreRdd).foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> tuple2) throws Exception {
                System.out.println("学生id: " + tuple2._1);
                System.out.println("学生名称: " + tuple2._2._1);
                System.out.println("学生成绩: "+ tuple2._2._2);
            }
        });
        sparkContext.close();
    }
}
