package com.java.spark.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/6.
 */
public class SortByKeyOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SortByKeyOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Tuple2<Integer,String>> list = Arrays.asList(
                new Tuple2<Integer, String>(100,"zhangsan"),
                new Tuple2<Integer, String>(98,"lisi"),
                new Tuple2<Integer, String>(99,"wangwu")
        );
        JavaPairRDD<Integer,String> javaPairRDD = sparkContext.parallelizePairs(list);
        //按key排序 参数是否升序
        javaPairRDD.sortByKey(false).foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> tuple2) throws Exception {
                System.out.println(tuple2._2);
            }
        });
        sparkContext.close();
    }
}
