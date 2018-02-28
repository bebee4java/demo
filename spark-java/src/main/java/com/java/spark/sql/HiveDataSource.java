package com.java.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * Created by sgr on 2018/2/26/026.
 */
public class HiveDataSource {
    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();
        //判断如果表存在则删除
        spark.sql("DROP TABLE IF EXISTS student_info");
        //判断如果表不存在则创建
        spark.sql("CREATE TABLE IF NOT EXISTS student_info(NAME STRING , AGE INT)");
        //加载数据
        spark.sql("LOAD DATA LOCAL INPATH '/opt/data/student_info.txt' INTO student_info");

        spark.sql("DROP TABLE IF EXISTS student_score");
        spark.sql("CREATE TABLE IF NOT EXISTS student_score(NAME STRING , SCORE INT)");
        spark.sql("LOAD DATA LOCAL INPATH '/opt/data/student_score.txt' INTO student_score");

        //关联两张表，查询分数大于80的学生
        Dataset<Row> df = spark.sql("select ss.name,si.age,ss.score from student_score ss join student_info si on ss.name = si.name where ss.score >= 80");
        //结果数据存回hive表
        spark.sql("DROP TABLE IF EXISTS good_student_info");
        df.createOrReplaceTempView("good_student_info");

        //读取hive表
        Dataset<Row> result = spark.table("good_student_info");
        Row[] rows = result.collect();
        for (Row row :rows){
            System.out.println(row);
        }
        spark.close();
    }
}
