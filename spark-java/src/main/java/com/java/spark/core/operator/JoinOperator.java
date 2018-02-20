package com.java.spark.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/10.
 */
public class JoinOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JoinOperator");
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
        nameRdd.join(socreRdd).foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> tuple2) throws Exception {
                System.out.println(tuple2._1 + " -> " + tuple2._2);
            }
        });
        sparkContext.close();
    }

}
