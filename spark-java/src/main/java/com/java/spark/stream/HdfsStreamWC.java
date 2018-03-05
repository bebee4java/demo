package com.java.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by sgr on 2018/3/4/004.
 */
public class HdfsStreamWC {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("HdfsStreamWC").setMaster("local[1]");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        JavaDStream<String> lines = jsc.textFileStream("hdfs://node1:8020/wordcount");
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        JavaPairDStream<String,Integer> wc = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //每次计算完成，打印一下统计情况
        wc.print();

        //结果存入myql表
        try {
            SqlConnectionPool.init("com.mysql.jdbc.Driver",
                    "jdbc:mysql://localhost:3306/test","root","root",10);
        }catch (Exception e){
            e.printStackTrace();
        }
        wc.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> wcRdd) throws Exception {
                //使用foreachPartition减少数据库连接
                wcRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                        Connection connection = SqlConnectionPool.getConnection();
                        Tuple2<String,Integer> tuple2 = null;
                        while (iterator.hasNext()){
                            tuple2 = iterator.next();
                            String sql = "insert into wordcount(word,count) values("+tuple2._1+","+tuple2._2+")";
                            Statement statement =  connection.createStatement();
                            statement.executeUpdate(sql);
                        }
                        SqlConnectionPool.returnConnection(connection);
                    }
                });
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
