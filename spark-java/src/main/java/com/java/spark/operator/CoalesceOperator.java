package com.java.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sgr on 2018/1/21/021.
 */
public class CoalesceOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("CoalesceOperator").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> list = Arrays.asList("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8");
        JavaRDD<String> javaRDD = sparkContext.parallelize(list, 6);
        JavaRDD<String> result = javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> temp = new ArrayList<String>();
                while (iterator.hasNext()){
                    temp.add("[" + (index+1) + "]:" + iterator.next());
                }
                return temp.iterator();
            }
        },true);

        //如果数据量少的时候可以使用collect将数据收集到diver端
        for (String s: result.collect()) {
            System.out.println(s);
        }
        /**
         * coalesce算子是将RDD的partition数量缩减，将一定的数据压缩到更少的partition分区中
         * 使用场景：
         * 一般在filter算子之后，数据量减少产生部分倾斜，会使用coalesce算子进行优化
         * coalesce算子会让数据更加紧凑。
         * 注意：是减少数据倾斜而不是消除
         *
         * 当shuffle=true时，可以将partition数量减少到很小（避免partition分布在多机的情况下）
         * 也可以将partition的数目由原先的增大，将会使用hash partitioner
         */
        JavaRDD<String> javaRDD1 =  result.coalesce(3);

        JavaRDD<String> result2 = javaRDD1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> temp = new ArrayList<String>();
                while (iterator.hasNext()){
                    temp.add("[" + (index+1) +"]" + iterator.next());
                }
                return temp.iterator();
            }
        },true);

        for (String s : result2.collect()){
            System.out.println(s);
        }

        sparkContext.close();
    }
}
