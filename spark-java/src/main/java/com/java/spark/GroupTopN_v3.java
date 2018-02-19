package com.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sgr on 2018/2/18/018.
 */
public class GroupTopN_v3 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("GroupTopN_v3");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 90),
                new Tuple2<String, Integer>("zhangsan", 99),
                new Tuple2<String, Integer>("zhangsan", 100),
                new Tuple2<String, Integer>("lisi", 99),
                new Tuple2<String, Integer>("lisi", 90),
                new Tuple2<String, Integer>("lisi", 91),
                new Tuple2<String, Integer>("wangwu", 90),
                new Tuple2<String, Integer>("wangwu", 90),
                new Tuple2<String, Integer>("wangwu", 91)
        );
        JavaPairRDD<String, Integer> javaPairRDD = sparkContext.parallelizePairs(list);
        JavaPairRDD<String, Iterable<Integer>> groupJavaPairRDD =  javaPairRDD.groupByKey();

        final List<String> keys = groupJavaPairRDD.map(new Function<Tuple2<String,Iterable<Integer>>, String>() {
            @Override
            public String call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                return tuple2._1;
            }
        }).collect();

        //一般不会这么操作，原因是使用了sortByKey,会有shuffle过程
        for (int i=0; i<keys.size(); i++){
            final int index = i;
            groupJavaPairRDD.filter(new Function<Tuple2<String, Iterable<Integer>>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                    return tuple2._1.equals(keys.get(index));
                }
            }).flatMap(new FlatMapFunction<Tuple2<String,Iterable<Integer>>, Integer>() {
                @Override
                public Iterator<Integer> call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                    return tuple2._2.iterator();
                }
            }).mapToPair(new PairFunction<Integer, Integer, String>() {
                @Override
                public Tuple2<Integer, String> call(Integer v) throws Exception {
                    return new Tuple2<Integer, String>(v, keys.get(index));
                }
            }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                    return new Tuple2<String, Integer>(tuple2._2, tuple2._1);
                }
            }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
                @Override
                public void call(Tuple2<String, Integer> tuple2) throws Exception {
                    System.out.println(tuple2._1 + " -> " + tuple2._2);
                }
            });
        }

        sparkContext.close();
    }
}
