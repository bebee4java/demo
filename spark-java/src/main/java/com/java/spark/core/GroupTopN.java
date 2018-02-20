package com.java.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by sgr on 2018/2/10.
 */
public class GroupTopN {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("GroupTopN");
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
        javaPairRDD.groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                List<Integer> list = new ArrayList<Integer>();
                Iterator<Integer> iterator = tuple2._2.iterator();
                //数据量小的时候可以采用 大量数据不能使用，因为需要把数据全量add到list
                while (iterator.hasNext()){
                    list.add(iterator.next());
                }
                Collections.sort(list, new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2 - o1;
                    }
                });
                List<Integer> sorces = list.subList(0, 3);//top 3
                return new Tuple2<String, Iterable<Integer>>(tuple2._1, sorces);
            }
        }).foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                System.out.println(tuple2._1 + " -> " + tuple2._2);
            }
        });

        sparkContext.close();
    }
}
