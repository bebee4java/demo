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
public class GroupTopN_v2 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("GroupTopN_v2");
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
        final int topN = 2;
        JavaPairRDD<String, Integer> javaPairRDD = sparkContext.parallelizePairs(list);
        //采用插入排序，固定topN大小数组，从而处理大量数据，占用固定空间
        javaPairRDD.groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                Integer[] temp = new Integer[topN];
                Arrays.fill(temp, Integer.MIN_VALUE);
                Iterator<Integer> iterator = tuple2._2.iterator();
                while (iterator.hasNext()){
                    Integer t = iterator.next();
                    for (int i=temp.length-1; i>=0; i--){
                        if (t > temp[i]){
                            //注意假设[0,i-1]都是有序(降序)的，如果待插入的元素比temp[i-1]还小则无需再与[i-1]前面的元素比较了，
                            //反之则进入if语句
                            int j;
                            for (j=i-1; j>=0 && temp[j]<=t; j--){
                                temp[j+1] = temp[j];//把比t小或者相等的元素全部往后移动一个位置
                            }
                            temp[j+1] = t;//把待排序的元素t插入腾出的位置(j+1)
                            break;
                        }
                    }
                }
                return new Tuple2<String, Iterable<Integer>>(tuple2._1, Arrays.asList(temp));
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
