package com.java.spark.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by sgr on 2018/2/8/008.
 */
public class CountByKeyOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("CountByKeyOperator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Tuple2<String,String>> list = Arrays.asList(
                new Tuple2<String, String>("浙江","杭州"),
                new Tuple2<String, String>("江苏","南京"),
                new Tuple2<String, String>("浙江","金华"),
                new Tuple2<String, String>("江苏","苏州"),
                new Tuple2<String, String>("北京","北京")
        );
        JavaPairRDD<String,String> javaPairRDD = sparkContext.parallelizePairs(list);
        //按key count action操作 有shuffle过程
        Map<String,Long> result =  javaPairRDD.countByKey();
        for (Map.Entry<String,Long> entry : result.entrySet()){
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        sparkContext.close();
    }
}
