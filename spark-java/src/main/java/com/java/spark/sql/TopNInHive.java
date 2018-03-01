package com.java.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * sparkSql读取hive数据做topN
 * 使用开窗函数
 * Created by sgr on 2018/3/1/001.
 */

public class TopNInHive {
    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("DROP TABLE IF EXISTS exam");
        spark.sql("CREATE TABLE IF NOT EXISTS exam(" +
                "name string,subject string,score int)");
        spark.sql("LOAD DATA LOCAL INPATH '/opt/data/exam.txt' INTO TABLE exam");
        //开窗函数的作用就是给每个分组的数据按其排序顺序，打上分组内的行号
        Dataset<Row> df = spark.sql("SELECT name,subject,score from (" +
                "SELECT name,subject,score row_number over(partition by subject order by score desc) rank" +
                "from exam) temp where rank <=3");
        spark.sql("DROP TABLE IF EXISTS top3_exam");
        df.createOrReplaceTempView("top3_exam");

        spark.close();
    }
}
