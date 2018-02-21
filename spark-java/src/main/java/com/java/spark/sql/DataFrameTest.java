package com.java.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by sgr on 2018/2/20/020.
 */
public class DataFrameTest {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("DataFrameTest")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        //当成表处理
        Dataset<Row> df = sparkSession.read().json("person.json");
        //打印表
        df.show();
        //打印元信息
        df.printSchema();
        //选择一列
        df.select("name").show();
        df.select(df.col("name"),df.col("age").plus(1)).show();

        //过滤
        df.filter(df.col("age").gt(10)).show();
        //根据某一列分组count
        df.groupBy("age").count().show();
        sparkSession.close();
    }
}
