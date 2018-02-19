package com.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgr on 2018/2/10.
 */
public class SecondSort {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SecondSort");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> list = Arrays.asList(
                "100 zhangsan 90",
                "100 lisi 100",
                "99 wangwu 90",
                "99 hanmeimei 100",
                "100 lilei 90",
                "102 zhaoliu 100"
        );
        JavaRDD<String> javaRDD = sparkContext.parallelize(list);

        JavaPairRDD<SecondSortKey,String> javaPairRDD = javaRDD.mapToPair(new PairFunction<String, SecondSortKey, String>() {
            @Override
            public Tuple2<SecondSortKey, String> call(String line) throws Exception {
                String[] ss = line.split(" ");
                SecondSortKey secondSortKey = new SecondSortKey(Integer.valueOf(ss[0]), Integer.valueOf(ss[2]));
                return new Tuple2<SecondSortKey, String>(secondSortKey, line);
            }
        });

        javaPairRDD.sortByKey(false).foreach(new VoidFunction<Tuple2<SecondSortKey, String>>() {
            @Override
            public void call(Tuple2<SecondSortKey, String> tuple2) throws Exception {
                System.out.println(tuple2._2);
            }
        });

        sparkContext.close();
    }
}
